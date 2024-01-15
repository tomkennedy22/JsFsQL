import { type_database, type_loose_query } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy } from "./utils";

type table_join_results = {
    data: any[];
    indexes: { [key: string]: { [key: string]: any } };
    groups: { [key: string]: { [key: string]: any[] } };
};

type join_results = {
    results: any[];
    tables: { [key: string]: table_join_results };
}

export const join = (
    db: type_database,
    base_table_name: string,
    include_table_names: string[],
    query_addons?: { [key: string]: type_loose_query },
    invert_from?: string
): any => {

    let join_tracker: join_results = { results: [], tables: {} };
    let all_tables_needed = new Set([base_table_name, ...include_table_names]);
    query_addons = query_addons || {};
    let join_from_table_results = join_for_table(db, base_table_name, all_tables_needed, query_addons, join_tracker);

    console.log('join_from_table_results - DONT HAVE INVERT FROM', { db, base_table: db.tables[base_table_name], base_table_conn: db.tables[base_table_name].table_connections, invert_from: invert_from, join_from_table_results, query_addons })

    if (invert_from) {
        all_tables_needed = new Set([base_table_name, ...include_table_names]);
        console.log('join_from_table_results', invert_from, join_from_table_results, query_addons)
        join_from_table_results = join_for_table(db, invert_from, all_tables_needed, query_addons, join_tracker);
    }

    return join_from_table_results;
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
    query_addons: { [key: string]: type_loose_query, },
    join_tracker: join_results
): join_results => {
    let table = db.tables[table_name];
    let table_connections = table.table_connections;
    let keys_to_index_by = table.get_foreign_keys_and_primary_keys();

    all_tables_needed.delete(table_name);
    let data: any[] = []
    if (join_tracker.tables[table_name] && false) {
        data = join_tracker.tables[table_name].data;
    }
    else {
        data = table.find(get_query_addons(query_addons, table_name)) || [];
    }

    join_tracker.tables[table_name] = {
        data,
        indexes: {},
        groups: {}
    }

    for (let key of keys_to_index_by) {
        join_tracker.tables[table_name].indexes[key] = index_by(data, key);
        join_tracker.tables[table_name].groups[key] = group_by(data, key);
    }

    console.log('join_for_table', { join_tracker, table_name, all_tables_needed, query_addons, table_connections })

    for (let connected_table_name in table_connections) {
        if (!all_tables_needed.has(connected_table_name)) {
            continue;
        }

        console.log('Looping connections of table', table_name, 'connected_table_name', connected_table_name, { query_addons })

        let connection = table_connections[connected_table_name];
        let connected_table = db.tables[connected_table_name];

        let join_key = connection.join_key;
        let parent_join_ids = distinct(data.map(row => get_from_dict(row, join_key)));

        let child_query = get_query_addons(query_addons, connected_table_name)
        let parent_id_query = { [join_key]: { $in: parent_join_ids } };

        let new_child_query = { ...child_query, ...parent_id_query };
        add_to_query_addons(query_addons, connected_table_name, new_child_query);

        join_tracker = join_for_table(db, connected_table_name, all_tables_needed, query_addons, join_tracker)
        let child_data_by_key: { [key: string]: any } = {};

        let store_key = '';

        if (connection.join_type === 'many_to_one') {
            child_data_by_key = join_tracker.tables[connected_table_name].indexes[join_key];
            store_key = connected_table_name;
        }
        else if (connection.join_type === 'one_to_many' || connection.join_type === 'one_to_one') {
            child_data_by_key = join_tracker.tables[connected_table_name].groups[join_key];
            store_key = `${connected_table_name}s`;
        }

        data = nest_children(data, child_data_by_key, join_key, store_key);
    }

    join_tracker.results = join_tracker.tables[table_name].data;
    return join_tracker;
}