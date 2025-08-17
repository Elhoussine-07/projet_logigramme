import json
from database import get_connection, get_rule_json, interpolate_missing_values
from collections import defaultdict
import math

def main():
    conn = get_connection()
    cursor = conn.cursor()

    # Charger la règle depuis la base
    json_text = get_rule_json(cursor, 1)
    if not json_text:
        raise Exception("Aucune règle trouvée dans la base")

    json_data = json.loads(json_text)
    blocks = json_data["blocks"]
    links = json_data["links"]

    # Associer un ID interne à chaque bloc
    id_to_block = {i + 1: block for i, block in enumerate(blocks)}

    # Création des maps d'entrées et de sorties
    inputs_map = defaultdict(list)
    outputs_map = defaultdict(list)
    for link in links:
        inputs_map[link["child"]].append(link["parent"])
        outputs_map[link["parent"]].append(link["child"])

    # Récupération de tous les IDs des variables sources
    variable_ids = [
        block["parameters"]["Id"]
        for block in blocks
        if block["class"] == "ReadVar"
    ]

    # Charger toutes les valeurs interpolées
    dated_values = interpolate_missing_values(cursor, variable_ids)
    var_index_map = {var_id: idx for idx, var_id in enumerate(variable_ids)}

    # Fonction récursive pour évaluer un bloc
    def evaluate_block(bid):
        block = id_to_block[bid]
        cls = block["class"]

        if cls == "ReadVar":
            var_id = block["parameters"]["Id"]
            idx = var_index_map[var_id]
            return [(date, values[idx]) for date, values in dated_values]

        elif cls in ('+', '-', '*', '/'):
            input_data_list = [evaluate_block(inp) for inp in inputs_map[bid]] 
        # input_data_list = [
        # [(2025-08-01, 10), (2025-08-02, 12)],   
        # [(2025-08-01, 5),  (2025-08-02, 7)] 
        # ]
            results = []
            for i in range(len(input_data_list[0])):
                date = input_data_list[0][i][0]
                vals = [inp[i][1] for inp in input_data_list]
                #le but ici c de rassembler les valeurs de chaque variable pour une date donnee afin d'appliquer operation sur eux
                if cls == '+':
                    res = sum(vals)
                elif cls == '-':
                    res = vals[0] - sum(vals[1:])
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
            input_data = evaluate_block(inputs_map[bid][0])
            operation = block["parameters"]["operation"].lower().strip()
            period_minutes = block["parameters"].get("period", 60)
            validity_rate = block["parameters"].get("validity_rate", 0)  # %
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

                # ⚠ Arrondir la date au début de l'heure (ex: 13:27 -> 13:00)
                aligned_date = dates[0].replace(minute=0, second=0, microsecond=0)
                results.append((aligned_date, res))

            results.sort(key=lambda x: x[0])
            return results


        elif cls == "WriteVar":
            results = evaluate_block(inputs_map[bid][0])
            var_id = block["parameters"]["Id"]
            for date, res in results:
                # On s'assure de ne pas dupliquer si déjà présent
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

    # Lancer le calcul pour chaque WriteVar
    end_blocks = [bid for bid, block in id_to_block.items() if block["class"] == "WriteVar"]
    for end_bid in end_blocks:
        evaluate_block(end_bid)

    # Marquer les variables sources comme qualifiées
    for date, _ in dated_values:
        for var_id in variable_ids:
            cursor.execute("""
                UPDATE his_valeur
                SET id_qualification = 1
                WHERE id_variable = ? AND date_acquisition = ? AND id_qualification = 0
            """, (var_id, date))

    conn.commit()
    print("Traitement terminé.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
