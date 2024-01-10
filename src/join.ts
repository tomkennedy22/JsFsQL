import { type_database, type_loose_query } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy } from "./utils";


export const join = (
                db: type_database, 
                base_table_name: string, 
                include_table_names: string[], 
                query_addons?: { [key: string]: type_loose_query },
                invert_from?: string
            ): any => {
    let all_tables_needed = new Set([base_table_name, ...include_table_names]);
    let join_from_table_results = join_for_table(db, base_table_name, all_tables_needed, query_addons);
    
    if (!invert_from){
        return join_from_table_results;
    }
    else {
        all_tables_needed = new Set([base_table_name, ...include_table_names]);
        return join_for_table(db, invert_from, all_tables_needed, query_addons);
    } 
}

const get_query_addons = (query_addons: { [key: string]: type_loose_query } | undefined, table_name: string) => {
    let query_addon = deep_copy(query_addons ? query_addons[table_name] || {} : {});
    return query_addon;
}

const add_to_query_addons = (query_addons: { [key: string]: type_loose_query } | undefined, table_name: string, query_addon: type_loose_query) => {

    if (!query_addons) {
        query_addons = {};
    }

    if (!query_addons[table_name]) {
        query_addons[table_name] = {};
    }

    query_addons[table_name] = { ...query_addons[table_name], ...query_addon };
}

const join_for_table = (
            db: type_database, 
            table_name: string, 
            all_tables_needed: Set<string>, 
            query_addons?: { [key: string]: type_loose_query }
        ): any[] => {
    let table = db.tables[table_name];
    let table_connections = table.table_connections;

    all_tables_needed.delete(table_name);
    let data = table.find(get_query_addons(query_addons, table_name)) || [];

    for (let connected_table_name in table_connections) {
        if (!all_tables_needed.has(connected_table_name)) {
            continue;
        }

        let connection = table_connections[connected_table_name];
        let connected_table = db.tables[connected_table_name];

        let join_key = connection.join_key;
        let parent_join_ids = distinct(data.map(row => get_from_dict(row, join_key)));

        let child_query = get_query_addons(query_addons, connected_table_name)
        let parent_id_query = { [join_key]: { $in: parent_join_ids } };

        let new_child_query = { ...child_query, ...parent_id_query };
        add_to_query_addons(query_addons, connected_table_name, new_child_query);

        let child_data: any[] = join_for_table(db, connected_table_name, all_tables_needed, query_addons)
        let child_data_by_key;

        let store_key = '';

        if (connection.join_type === 'many_to_one') {
            child_data_by_key = index_by(child_data, join_key);
            store_key = connected_table_name;
        }
        else if (connection.join_type === 'one_to_many' || connection.join_type === 'one_to_one') {
            child_data_by_key = group_by(child_data, join_key);
            store_key = `${connected_table_name}s`;
        }

        data = nest_children(data, child_data_by_key, join_key, store_key);
    }

    return data;
}