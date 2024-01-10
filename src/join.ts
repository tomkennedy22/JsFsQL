import { type_database, type_loose_query } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy } from "./utils";


export const join = (db: type_database, base_table_name: string, include_table_names: string[], query_addons?: { [key: string]: type_loose_query }): any => {
    let all_tables_needed = new Set([base_table_name, ...include_table_names]);
    return join_for_table(db, base_table_name, all_tables_needed, query_addons);
}

const get_query_addons = (query_addons: { [key: string]: type_loose_query } | undefined, table_name: string) => {
    let query_addon = deep_copy(query_addons ? query_addons[table_name] || {} : {});
    console.log('get_query_addons', { query_addons, table_name, query_addon })
    return query_addon;
}

const add_to_query_addons = (query_addons: { [key: string]: type_loose_query } | undefined, table_name: string, query_addon: type_loose_query) => {
    if (!query_addons) {
        query_addons = {};
    }

    if (!query_addons[table_name]) {
        query_addons[table_name] = {};
    }

    console.log('add_to_query_addons', { query_addons, table_name, query_addon })

    query_addons[table_name] = { ...query_addons[table_name], ...query_addon };
}

const join_for_table = (db: type_database, table_name: string, all_tables_needed: Set<string>, query_addons?: { [key: string]: type_loose_query }): any[] => {
    let table = db.tables[table_name];
    let table_connections = table.table_connections;

    all_tables_needed.delete(table_name);
    let data = table.find(get_query_addons(query_addons, table_name)) || [];

    console.log('join_for_table', { table_name, all_tables_needed, query_addons, data })

    for (let connected_table_name in table_connections) {
        if (!all_tables_needed.has(connected_table_name)) {
            continue;
        }


        console.log('join_for_table - loop connected tables', { connected_table_name })

        let connection = table_connections[connected_table_name];
        let connected_table = db.tables[connected_table_name];

        let join_key = connection.join_key;
        let parent_join_ids = distinct(data.map(row => get_from_dict(row, join_key)));

        console.log('join_for_table - after parent IDs', { connected_table_name, join_key, parent_join_ids, query_addons })

        let child_query = get_query_addons(query_addons, connected_table_name)
        let parent_id_query = { [join_key]: { $in: parent_join_ids } };

        let new_child_query = { ...child_query, ...parent_id_query };
        console.log('Before query addons', { connected_table_name, child_query, parent_id_query, new_child_query })
        add_to_query_addons(query_addons, connected_table_name, new_child_query);

        console.log('Child query', { child_query, query_addons, parent_id_query })

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

        console.log('join_for_table - after child data', { child_data, child_data_by_key, store_key })

        data = nest_children(data, child_data_by_key, join_key, store_key);
    }

    return data;
}