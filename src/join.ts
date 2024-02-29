import { type_database, type_loose_query } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy, first_element } from "./utils";

type table_join_results = {
    data: any[];
    indexes: { [key: string]: { [key: string]: any } };
    groups: { [key: string]: { [key: string]: any[] } };
};

type join_results = {
    results: any[];
    tables: { [key: string]: table_join_results };
}


type type_join_criteria = {
    filter?: type_loose_query;
    sort?: any;
    find_fn?: 'find' | 'findOne';
    name?: string;
    filter_up?: boolean;
    children?: { [key: string]: type_join_criteria };
}

export const nested_join = (db: type_database, join_criteria: { [key: string]: type_join_criteria }): any[] => {
    return recurse_nested_join(db, null, [], join_criteria);
}


export const recurse_nested_join = (db: type_database, parent_name: string | null, parent_array: any[], join_criteria_dict: { [key: string]: type_join_criteria }): any[] => {
    for (let table_name in join_criteria_dict) {
        let table = db.tables[table_name];

        let table_connections = table.table_connections;

        let criteria = join_criteria_dict[table_name] || {};
        let find_fn: 'find' | 'findOne' = criteria.find_fn || 'find';
        let filter = criteria.filter || {};
        let name = criteria.name || table_name;
        let sort = criteria.sort || {};

        console.log('recurse_nested_join', { parent_name, join_criteria_dict, table_name, table_connections: table.table_connections, criteria, parent_array })
        let parent_connection: any;
        let parent_join_key: string | null = null;
        if (parent_name) {
            parent_connection = table.table_connections[parent_name];
            parent_join_key = parent_connection.join_key;
            console.log('parent_connection', { parent_connection, parent_join_key, parent_array })
            if (parent_join_key) {
                let parent_values = distinct(parent_array.map(row => get_from_dict(row, parent_join_key as string))).filter(value => value);

                if (parent_values.length > 0) {
                    filter[parent_join_key] = { $in: parent_values };
                }
            }
        }

        let data = table.find(filter);
        let data_by_join_key;

        if (parent_name && parent_connection && parent_join_key) {
            if (parent_connection.join_type === 'one_to_many' || parent_connection.join_type === 'one_to_one') {
                data_by_join_key = index_by(data, parent_join_key);
            }
            else if (parent_connection.join_type === 'many_to_one') {
                data_by_join_key = group_by(data, parent_join_key);

            }
        }


        if (criteria.children) {
            recurse_nested_join(db, table_name, data, criteria.children);
        }

        if (parent_name && parent_connection && parent_join_key && parent_connection.join_type === 'many_to_one' && criteria.sort) {
            for (let key in data_by_join_key) {
                data_by_join_key[key] = data_by_join_key[key].sort((a: any, b: any) => {
                    for (let sort_key in criteria.sort) {
                        let sort_val = criteria.sort[sort_key];
                        let val_a = get_from_dict(a, sort_key);
                        let val_b = get_from_dict(b, sort_key);
                        if (val_a < val_b) {
                            return sort_val;
                        }
                        else if (val_a > val_b) {
                            return sort_val * -1;
                        }
                    }
                    return 0;
                });
            }
        }

        if (parent_name && parent_connection && parent_array && parent_join_key) {
            parent_array = nest_children(parent_array, data_by_join_key, parent_join_key, name);
            if (criteria.filter_up) {
                parent_array = parent_array.filter(row => row[name]);
            }
        }
        else {
            parent_array = find_fn == 'find' ? data : first_element(data);
        }
    }

    return parent_array;
}