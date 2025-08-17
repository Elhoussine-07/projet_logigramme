from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
import json
from datetime import datetime
import logging
from collections import defaultdict
import math

app = Flask(__name__)
CORS(app)  # Enable CORS for all domains

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    """Establish connection to SQL Server database"""
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-FOIB0OT;'
        'DATABASE=DATARULE;'
        'Trusted_Connection=yes;'
    )

def get_rule_json(cursor, id_rule):
    """Get rule JSON from ref_regle table"""
    cursor.execute("SELECT text_json FROM ref_regle WHERE id_regle=?", id_rule)
    row = cursor.fetchone()
    return row[0] if row else None

def interpolate_missing_values(cursor, variable_ids):
    """Interpolate missing values for given variable IDs"""
    dates_by_var = {}
    all_dates = set()
    values_by_var = {}

    for var_id in variable_ids:
        cursor.execute("""
            SELECT date_acquisition, val_valide
            FROM his_valeur
            WHERE id_variable = ? AND id_qualification = 0
        """, (var_id,))
        rows = cursor.fetchall()
        dates_by_var[var_id] = {r[0] for r in rows}
        values_by_var[var_id] = {r[0]: r[1] for r in rows}
        all_dates.update(dates_by_var[var_id])

    all_dates = sorted(all_dates)

    for var_id in variable_ids:
        for date in all_dates:
            if date not in values_by_var[var_id]:
                prev_dates = [d for d in dates_by_var[var_id] if d < date]
                next_dates = [d for d in dates_by_var[var_id] if d > date]

                if prev_dates and next_dates:
                    d1 = max(prev_dates)
                    d2 = min(next_dates)
                    v1 = values_by_var[var_id][d1]
                    v2 = values_by_var[var_id][d2]

                    t1 = d1.timestamp()
                    t2 = d2.timestamp()
                    t = date.timestamp()
                    interpolated_value = v1 + (v2 - v1) * (t - t1) / (t2 - t1)
                    values_by_var[var_id][date] = interpolated_value
                elif prev_dates:
                    values_by_var[var_id][date] = values_by_var[var_id][max(prev_dates)]
                elif next_dates:
                    values_by_var[var_id][date] = values_by_var[var_id][min(next_dates)]

    complete_results = []
    for date in all_dates:
        row_values = [values_by_var[var_id][date] for var_id in variable_ids]
        complete_results.append((date, row_values))

    return complete_results

def execute_rule_logic(cursor, json_data):
    """Execute the rule logic from JSON data"""
    try:
        blocks = json_data["blocks"]
        links = json_data["links"]

        # Créer un mapping des blocs par leur index (position dans le tableau)
        # L'ID du bloc correspond à son index + 1
        id_to_block = {i + 1: block for i, block in enumerate(blocks)}

        # Création des maps d'entrées et de sorties basées sur les IDs des liens
        inputs_map = defaultdict(list)
        outputs_map = defaultdict(list)
        for link in links:
            inputs_map[link["child"]].append(link["parent"])
            outputs_map[link["parent"]].append(link["child"])

        # Récupération de tous les IDs des variables sources
        variable_ids = []
        for i, block in enumerate(blocks):
            if block["class"] == "ReadVar":
                variable_ids.append(block["parameters"]["Id"])

        if not variable_ids:
            return {"error": "No ReadVar blocks found in the rule"}

        # Charger toutes les valeurs interpolées
        dated_values = interpolate_missing_values(cursor, variable_ids)
        var_index_map = {var_id: idx for idx, var_id in enumerate(variable_ids)}

        # Fonction récursive pour évaluer un bloc par son ID
        def evaluate_block(block_id):
            if block_id not in id_to_block:
                raise ValueError(f"Block ID {block_id} not found")
                
            block = id_to_block[block_id]
            cls = block["class"]

            if cls == "ReadVar":
                var_id = block["parameters"]["Id"]
                if var_id not in var_index_map:
                    raise ValueError(f"Variable ID {var_id} not found in data")
                idx = var_index_map[var_id]
                return [(date, values[idx]) for date, values in dated_values]

            elif cls in ('+', '-', '*', '/'):
                input_block_ids = inputs_map[block_id]
                if not input_block_ids:
                    raise ValueError(f"No inputs found for operation block {block_id}")
                    
                input_data_list = [evaluate_block(inp) for inp in input_block_ids]
                
                if not input_data_list:
                    return []
                    
                results = []
                min_length = min(len(data) for data in input_data_list)
                
                for i in range(min_length):
                    date = input_data_list[0][i][0]
                    vals = [inp[i][1] for inp in input_data_list if inp[i][1] is not None]
                    
                    if not vals:
                        results.append((date, None))
                        continue
                    
                    if cls == '+':
                        res = sum(vals)
                    elif cls == '-':
                        res = vals[0] - sum(vals[1:]) if len(vals) > 1 else vals[0]
                    elif cls == '*':
                        res = math.prod(vals)
                    elif cls == '/':
                        res = vals[0]
                        for v in vals[1:]:
                            if v == 0:
                                res = None
                                break
                            res /= v
                    
                    results.append((date, res))
                return results

            elif cls == "PeriodicCalc":
                input_block_ids = inputs_map[block_id]
                if not input_block_ids:
                    raise ValueError(f"No input found for PeriodicCalc block {block_id}")
                    
                input_data = evaluate_block(input_block_ids[0])
                operation = block["parameters"]["operation"].lower().strip()
                period_minutes = block["parameters"].get("period", 60)
                validity_rate = block["parameters"].get("validity_rate", 0)
                period_seconds = period_minutes * 60

                if not input_data:
                    return []

                grouped_data = defaultdict(list)
                for date, value in input_data:
                    period_index = math.floor(date.timestamp() / period_seconds)
                    grouped_data[period_index].append((date, value))

                results = []
                for group_idx, group_values in grouped_data.items():
                    dates = [d for d, _ in group_values]
                    vals = [v for _, v in group_values if v is not None]

                    total_points = len(group_values)
                    valid_points = len(vals)
                    if total_points == 0:
                        continue
                    percentage_valid = (valid_points / total_points) * 100
                    if percentage_valid < validity_rate:
                        continue

                    if not vals:
                        continue

                    if operation == "moyenne":
                        res = sum(vals) / len(vals)
                    elif operation == "somme":
                        res = sum(vals)
                    elif operation == "maximum":
                        res = max(vals)
                    elif operation == "minimum":
                        res = min(vals)
                    elif operation == "premiere":
                        res = vals[0]
                    elif operation == "derniere":
                        res = vals[-1]
                    else:
                        raise ValueError(f"Opération périodique inconnue : {operation}")

                    # Utiliser la première date du groupe pour l'alignement
                    aligned_date = min(dates).replace(minute=0, second=0, microsecond=0)
                    results.append((aligned_date, res))

                results.sort(key=lambda x: x[0])
                return results

            elif cls == "WriteVar":
                input_block_ids = inputs_map[block_id]
                if not input_block_ids:
                    raise ValueError(f"No input found for WriteVar block {block_id}")
                    
                results = evaluate_block(input_block_ids[0])
                var_id = block["parameters"]["Id"]
                
                for date, res in results:
                    if res is not None:  # Only write non-null values
                        cursor.execute("""
                            IF NOT EXISTS (
                                SELECT 1 FROM his_valeur 
                                WHERE id_variable = ? AND date_acquisition = ?
                            )
                            INSERT INTO his_valeur (
                                id_variable, date_acquisition, id_qualification, date_insertion, val_brute, val_valide
                            )
                            VALUES (?, ?, 1, GETDATE(), ?, ?)
                        """, (var_id, date, var_id, date, res, res))
                return results

            else:
                raise ValueError(f"Type de bloc inconnu: {cls}")

        # Lancer le calcul pour chaque WriteVar (identifier par leur ID)
        end_block_ids = []
        for i, block in enumerate(blocks):
            if block["class"] == "WriteVar":
                end_block_ids.append(i + 1)  # ID = index + 1
        
        execution_results = []
        for end_block_id in end_block_ids:
            block_results = evaluate_block(end_block_id)
            execution_results.extend(block_results)

        # Marquer les variables sources comme qualifiées
        for date, _ in dated_values:
            for var_id in variable_ids:
                cursor.execute("""
                    UPDATE his_valeur
                    SET id_qualification = 1
                    WHERE id_variable = ? AND date_acquisition = ? AND id_qualification = 0
                """, (var_id, date))

        return {
            "success": True,
            "processed_dates": len(dated_values),
            "output_values": len(execution_results),
            "variable_ids_processed": variable_ids
        }

    except Exception as e:
        logger.error(f"Error executing rule logic: {str(e)}")
        return {"error": str(e)}

@app.route('/api/save-rule', methods=['POST'])
def save_rule():
    """Save rule JSON data directly to ref_regle table"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        json_data = data.get('json_data')
        rule_name = data.get('name', '')
        rule_description = data.get('description', '')
        id_regle = data.get('id_regle')  # Optional for new rules
        
        if not json_data:
            return jsonify({'error': 'json_data is required'}), 400
        
        # Convert dict to JSON string if needed
        if isinstance(json_data, dict):
            # Extract name and description from json_data if not provided
            if not rule_name and 'name' in json_data:
                rule_name = json_data['name']
            if not rule_description and 'description' in json_data:
                rule_description = json_data['description']
            
            # Ensure the JSON has the required structure
            if 'id' not in json_data:
                json_data['id'] = id_regle if id_regle else -1
            if 'name' not in json_data:
                json_data['name'] = rule_name
            if 'description' not in json_data:
                json_data['description'] = rule_description
            if 'blocks' not in json_data:
                json_data['blocks'] = []
            if 'links' not in json_data:
                json_data['links'] = []
            
            json_data_str = json.dumps(json_data, ensure_ascii=False)
        else:
            json_data_str = str(json_data)
        
        # Set default values if empty
        if not rule_name:
            rule_name = f'Rule_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        
        conn = get_connection()
        cursor = conn.cursor()
        
        if id_regle:
            # Check if rule exists for update
            cursor.execute("SELECT COUNT(*) FROM ref_regle WHERE id_regle = ?", (id_regle,))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # Update existing rule
                # Update the JSON with the correct ID
                if isinstance(json_data, dict):
                    json_data['id'] = id_regle
                    json_data_str = json.dumps(json_data, ensure_ascii=False)
                
                cursor.execute("""
                    UPDATE ref_regle 
                    SET text_json = ?, 
                        lib_nom = ?
                    WHERE id_regle = ?
                """, (json_data_str, rule_name, id_regle))
                message = f'Rule {id_regle} updated successfully'
            else:
                # Insert new rule with specified ID
                # Update the JSON with the correct ID
                if isinstance(json_data, dict):
                    json_data['id'] = id_regle
                    json_data_str = json.dumps(json_data, ensure_ascii=False)
                
                cursor.execute("""
                    INSERT INTO ref_regle (id_regle, lib_nom, est_modele, text_json)
                    VALUES (?, ?, 0, ?)
                """, (id_regle, rule_name, json_data_str))
                message = f'Rule {id_regle} created successfully'
        else:
            # Insert new rule with auto-generated ID
            cursor.execute("""
                INSERT INTO ref_regle (lib_nom, est_modele, text_json)
                VALUES (?, 0, ?)
            """, (rule_name, json_data_str))
            
            # Get the generated ID
            cursor.execute("SELECT @@IDENTITY")
            id_regle = cursor.fetchone()[0]
            
            # Update the JSON with the generated ID
            if isinstance(json_data, dict):
                json_data['id'] = int(id_regle)
                json_data_str = json.dumps(json_data, ensure_ascii=False)
                
                # Update the record with the correct JSON
                cursor.execute("""
                    UPDATE ref_regle 
                    SET text_json = ?
                    WHERE id_regle = ?
                """, (json_data_str, id_regle))
            
            message = f'New rule created successfully with ID: {id_regle}'
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Rule {id_regle} saved successfully")
        
        return jsonify({
            'success': True,
            'message': message,
            'id_regle': int(id_regle),
            'name': rule_name
        }), 200
        
    except Exception as e:
        logger.error(f"Error saving rule: {str(e)}")
        return jsonify({
            'error': 'Failed to save rule',
            'details': str(e)
        }), 500

@app.route('/api/get-rule/<int:rule_id>', methods=['GET'])
def get_rule(rule_id):
    """Retrieve a specific rule by ID from ref_regle table"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM ref_regle WHERE id_regle = ?", (rule_id,))
        row = cursor.fetchone()
        
        if not row:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Rule not found'}), 404
        
        # Get column names
        columns = [column[0] for column in cursor.description]
        
        # Create rule dictionary
        rule = dict(zip(columns, row))
        
        # Parse JSON data if it exists
        if rule.get('text_json'):
            try:
                rule['json_data'] = json.loads(rule['text_json'])
            except json.JSONDecodeError:
                rule['json_data'] = rule['text_json']
        
        cursor.close()
        conn.close()
        
        return jsonify(rule), 200
        
    except Exception as e:
        logger.error(f"Error retrieving rule: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve rule',
            'details': str(e)
        }), 500

@app.route('/api/get-rules', methods=['GET'])
def get_rules():
    """Retrieve all rules from ref_regle table"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM ref_regle ORDER BY id_regle")
        rows = cursor.fetchall()
        
        if not rows:
            cursor.close()
            conn.close()
            return jsonify({
                'rules': [],
                'count': 0
            }), 200
        
        # Get column names
        columns = [column[0] for column in cursor.description]
        
        rules = []
        for row in rows:
            rule = dict(zip(columns, row))
            
            # Parse JSON data if it exists
            if rule.get('text_json'):
                try:
                    rule['json_data'] = json.loads(rule['text_json'])
                    rule['has_json'] = True
                except json.JSONDecodeError:
                    rule['has_json'] = False
            else:
                rule['has_json'] = False
            
            rules.append(rule)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'rules': rules,
            'count': len(rules)
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving rules: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve rules',
            'details': str(e)
        }), 500

@app.route('/api/execute-rule/<int:rule_id>', methods=['POST'])
def execute_rule_by_id(rule_id):
    """Execute a rule from ref_regle table by ID"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Get rule JSON from ref_regle
        json_text = get_rule_json(cursor, rule_id)
        if not json_text:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Rule with ID {rule_id} not found'}), 404

        json_data = json.loads(json_text)
        
        # Execute the rule logic
        result = execute_rule_logic(cursor, json_data)
        
        if "error" in result:
            cursor.close()
            conn.close()
            return jsonify(result), 500
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Rule {rule_id} executed successfully")
        
        return jsonify({
            'success': True,
            'message': f'Rule {rule_id} executed successfully',
            'rule_id': rule_id,
            'execution_details': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error executing rule {rule_id}: {str(e)}")
        return jsonify({
            'error': 'Failed to execute rule',
            'details': str(e)
        }), 500

@app.route('/api/simulate-rule', methods=['POST'])
def simulate_rule():
    """Simulate a rule from JSON data without saving to database"""
    try:
        data = request.get_json()
        
        if not data or 'json_data' not in data:
            return jsonify({'error': 'json_data is required'}), 400
        
        json_data = data['json_data']
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Execute the rule logic in simulation mode (no commits)
        result = execute_rule_logic(cursor, json_data)
        
        cursor.close()
        conn.close()
        
        if "error" in result:
            return jsonify(result), 500
        
        logger.info("Rule simulation completed successfully")
        
        return jsonify({
            'success': True,
            'message': 'Rule simulation completed successfully',
            'simulation_results': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error simulating rule: {str(e)}")
        return jsonify({
            'error': 'Failed to simulate rule',
            'details': str(e)
        }), 500

@app.route('/api/delete-rule/<int:rule_id>', methods=['DELETE'])
def delete_rule(rule_id):
    """Delete a rule from ref_regle table (only clears text_JSON)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Check if rule exists
        cursor.execute("SELECT COUNT(*) FROM ref_regle WHERE id_regle = ?", (rule_id,))
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Rule with ID {rule_id} not found'}), 404
        
        # Clear the JSON data (or delete the entire row if needed)
        cursor.execute("""
            UPDATE ref_regle 
            SET text_json = NULL
            WHERE id_regle = ?
        """, (rule_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Rule {rule_id} JSON data cleared successfully")
        
        return jsonify({
            'success': True,
            'message': f'Rule {rule_id} JSON data cleared successfully',
            'rule_id': rule_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error deleting rule {rule_id}: {str(e)}")
        return jsonify({
            'error': 'Failed to delete rule',
            'details': str(e)
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 503

if __name__ == '__main__':
    logger.info("Starting Flask API - Direct ref_regle integration")
    app.run(debug=True, host='0.0.0.0', port=5000)