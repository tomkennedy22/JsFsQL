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

const most_precise_query_table = (query_addons: type_loose_query): string | null => {
    let maxKey = null;
    let maxCount = 0;

    for (const key in query_addons) {
        const count = Object.keys(query_addons[key]).length;
        if (count > maxCount) {
            maxCount = count;
            maxKey = key;
        }
    }

    return maxKey;
}

export const highest_parent = (db: type_database, table_names: string[], query_addons: { [key: string]: type_loose_query }): string => {
    let tables_without_parent_set = new Set(table_names);

    for (let table_name of table_names) {
        let table = db.tables[table_name];
        let table_connections = table.table_connections;

        for (let connected_table_name in table_connections) {
            let connection = table_connections[connected_table_name];
            if (connection.join_type === 'many_to_one' && table_names.includes(connected_table_name)) {
                tables_without_parent_set.delete(table_name);
            }
        }
    }

    let tables_without_parent = [...tables_without_parent_set];
    if (tables_without_parent.length === 0) {
        if (Object.keys(query_addons).length > 0) {
            console.log('No tables without parent found, but query addons are present');
            return most_precise_query_table(query_addons) || table_names[0];
        }
    }
    else if (tables_without_parent.length > 1) {
        let tables_without_parent_with_query_addons = tables_without_parent.filter(table_name => query_addons[table_name]);
        if (tables_without_parent_with_query_addons.length === 0) {
            throw new Error('Multiple tables without parent found, but none have query addons');
        }
        else {
            return tables_without_parent_with_query_addons[0];
        }
    }
    else {
        return tables_without_parent[0];
    }

    return table_names[0]
}

export const join = (
    db: type_database,
    base_table_name: string,
    include_table_names: string[],
    query_addons?: { [key: string]: type_loose_query },
): join_results => {

    let join_tracker: join_results = { results: [], tables: {} };
    let all_tables_needed = new Set([base_table_name, ...include_table_names]);

    query_addons = query_addons || {};
    let first_table = highest_parent(db, [...all_tables_needed], query_addons);
    console.log('join', { first_table, base_table_name, include_table_names, query_addons })

    let join_from_table_results = join_for_table(db, first_table, all_tables_needed, query_addons, join_tracker);

    console.log('join_from_table_results - DONT HAVE INVERT FROM', { base_table_name, base_table_conn: db.tables[base_table_name].table_connections, join_from_table_results, query_addons })

    if (first_table !== base_table_name) {
        all_tables_needed = new Set([base_table_name, ...include_table_names]);
        console.log('join_from_table_results', base_table_name, join_from_table_results, query_addons)
        join_from_table_results = join_for_table(db, base_table_name, all_tables_needed, query_addons, join_tracker);
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

    for (let key in query_addon) {
        if (!query_addons[table_name][key]) {
            query_addons[table_name][key] = query_addon[key];
        }
    }
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

    // console.log('join_for_table', { join_tracker, table_name, all_tables_needed, query_addons, table_connections })

    for (let connected_table_name in table_connections) {
        if (!all_tables_needed.has(connected_table_name)) {
            continue;
        }

        // console.log('Looping connections of table', table_name, 'connected_table_name', connected_table_name, { query_addons })

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