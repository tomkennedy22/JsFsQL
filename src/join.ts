import { type_join_pattern, type_database, type_join_criteria } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children } from "./utils";

export const join = (db: any, base_table_name: string, join_pattern: type_join_pattern): any => {
    return process_table(db, base_table_name, join_pattern[base_table_name]);
}


export const process_table = (
    db: type_database,
    table_name: string,
    join_criteria: type_join_criteria
): any[] => {

    let data: any[];
    let db_table = db.tables[table_name];

    data = db_table.find(join_criteria.query || {}) || [];

    if (join_criteria.children) {
        Object.keys(join_criteria.children).forEach(child_table => {
            let child_db_table = db.tables[child_table];

            let join_key = db_table.get_foreign_key(child_table) || child_db_table.get_foreign_key(table_name) || 'NO JOIN KEY';
            let parent_join_ids = distinct(data.map(row => get_from_dict(row, join_key)));

            let child_obj = join_criteria.children![child_table];
            child_obj.query = child_obj.query || {};
            let parent_id_query = { [join_key]: { $in: parent_join_ids } };
            child_obj.query = { ...child_obj.query, ...parent_id_query };

            let child_data: any[] = process_table(db, child_table, join_criteria.children![child_table]);
            let child_data_by_key;

            let store_key = '';

            if (child_obj.type === 'single') {
                child_data_by_key = index_by(child_data, join_key);
                store_key = child_table;
            }
            else if (child_obj.type === 'group') {
                child_data_by_key = group_by(child_data, join_key);
                store_key = `${child_table}s`;
            }
            
            data = nest_children(data, child_data_by_key, join_key, store_key);
        });
    }

    return data;

}