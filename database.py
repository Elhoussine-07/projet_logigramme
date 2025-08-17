import pyodbc

def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-FOIB0OT;'
        'DATABASE=DATARULE;'
        'Trusted_Connection=yes;'
    )

def get_rule_json(cursor, id_rule):
    cursor.execute("SELECT text_JSON FROM ref_regle WHERE id_regle=?", id_rule)
    row = cursor.fetchone()
    return row[0] if row else None

def interpolate_missing_values(cursor, variable_ids):
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
