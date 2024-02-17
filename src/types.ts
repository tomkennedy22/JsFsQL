import { results } from "./results";

// Type alias for a partition index, which is a map of keys to arbitrary values.
export type type_partition_metadata = { [key: string]: any };

export type type_results = {
    [key: string]: any;
    left_join(right_dataset: type_results | type_table,
        join: string | { left_key: string, right_key: string },
        map_style: string,
        map_keys: { left_field?: string, right_field: string }): type_results;
    index_by(index_field: string): { [key: string]: any };
    group_by(group_by_field: string): { [key: string]: any };
    first(): any;

}[];

// Type definition for a Partition, outlining its structure and methods.
export type type_partition = {
    partition_name: string;
    partition_metadata: type_partition_metadata;
    storage_location: string;
    proto: any;
    json_output_file_path: string;
    txt_output_file_path: string;
    data: { [key: string]: any };
    primary_key: string;
    is_dirty: boolean; // Default is_dirty to true to indicate the partition requires saving upon creation.
    write_lock: boolean; // Default write_lock to false to indicate the partition is not currently being saved.
    do_compression: boolean;

    last_update_dt: Date;

    insert: (data: any) => void;
    update: (data: any[] | any, fields_to_drop?: any[]) => void;
    write_to_file: () => Promise<void>;
    delete_file: () => Promise<void>;
}


export type type_join_type = 'one_to_one' | 'one_to_many' | 'many_to_one';
export type type_join_criteria = {
    join_key: string;
    join_type: type_join_type;
}

// Defines the structure for a database table, including its name, partitions, storage strategy, and primary key.
export type type_table = {
    table_name: string; // Unique identifier for the table.
    partition_keys: string[]; // List of fields indexed for efficient querying.
    index_keys?: string[]; // List of fields indexed for efficient querying.
    storage_location: string; // File system path where table data is stored.
    output_file_path: string; // File system path where table data is stored.
    primary_key: string; // Field used to uniquely identify records within the table.
    proto?: any; // Prototype object used to instantiate new records.
    do_compression:boolean;

    table_connections: { [key: string]: {join_key: string, join_type: type_join_type} };

    // Mappings for partition management based on partition names and primary keys.
    partitions_by_partition_name: { [key: string]: type_partition };
    partition_name_by_primary_key: { [key: string]: string };
    partition_names_by_index: { [key: string]: { [key: string]: Set<string> } };

    delete_key_list: string[];

    // Methods for data manipulation and retrieval.
    find_partitions: () => type_partition[];
    read_from_file: () => Promise<void>;
    normalize_query: (query: type_query) => type_query;
    primary_key_partition_filter: (partitions: type_partition[], query_clause: type_query_clause) => type_partition[];
    clear: () => Promise<void>;
    next_id: () => number;
    count: () => number;
    insert: (data: any[] | any) => void;
    update: (data: any[] | any, fields_to_drop?: any[]) => void;
    delete: (query?: type_loose_query) => void;
    find: (query?: type_loose_query) => results<any>;
    cleanse_before_alter: (data: any[]) => any[];
    findOne: (query?: type_loose_query) => any;
    output_to_file: () => Promise<void>;
    filter: (data: any, query_field: type_query_field, query_clause: type_query_clause) => any[];
    partition_filter: (partitions: type_partition[], index_name: string, query_clause: type_query_clause) => type_partition[];
    get_table_connection: (foreign_table_name: string) => type_join_criteria | null;
    get_all_foreign_keys: () => string[];
    get_foreign_keys_and_primary_keys: () => string[];
}

// Defines a query field as a string, representing the field to query within data records.
export type type_query_field = string;

// Enumerates the possible query functions that can be used in a query clause.
export type type_query_function = '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin';

// Represents the structure for a query clause, mapping query functions to their corresponding values.
export type type_query_clause = {
    [key in type_query_function]?: any;
}

// Describes a loose query object that can have any string as a key and any type as a value.
export type type_loose_query = {
    [key: string]: any;
}

// Represents a more specific query structure where keys are tied to either a specific value or a query clause.
export type type_query = {
    [key: string]: number | string | type_query_clause;
}

export type type_table_init = {
    table_name: string;
    partition_keys: string[];
    index_keys?: string[];
    storage_location?: string;
    dbname?: string;
    primary_key: string;
    proto?: any;
    delete_key_list: string[];
    do_compression?: boolean;
}

export type type_connection_init = {
    table_a_name: string;
    table_b_name: string;
    join_key: string;
    join_type: type_join_type;
}

export type type_database = {
    dbname: string;
    tables: { [key: string]: type_table };
    folder_path: string;
    storage_location: string;
    output_file_path: string;

    add_table: ({ table_name, partition_keys, index_keys, primary_key, proto }: type_table_init) => type_table;
    add_connection: ({ table_a_name, table_b_name, join_key, join_type }: type_connection_init) => void;
    save_database: () => Promise<void>;
    read_from_file: () => Promise<void>;
}