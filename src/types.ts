// Type alias for a partition index, which is a map of keys to arbitrary values.
type PartitionIndex = { [key: string]: any };

// Type definition for a Partition, outlining its structure and methods.
type Partition = {
    partition_name: string;
    partition_indices: PartitionIndex;
    data: { [key: string]: any };
    storage_location: string;
    primary_key: string;
    is_dirty: boolean;
    insert: (data: any[] | any) => void;
    write_to_file: () => Promise<void>;
}


// Defines the structure for a database table, including its name, indices, storage strategy, and primary key.
type Table = {
    table_name: string; // Unique identifier for the table.
    indices: string[]; // List of fields indexed for efficient querying.
    storage_location: string; // File system path where table data is stored.
    primary_key: string; // Field used to uniquely identify records within the table.

    // Mappings for partition management based on partition names and primary keys.
    partitions_by_partition_name: { [key: string]: Partition };
    partition_name_by_primary_key: { [key: string]: string };

    // Methods for data manipulation and retrieval.
    insert: (data: any[]) => void;
    find: (query: LooseQuery) => any[];
    findOne: (query: LooseQuery) => any;
    output_to_file: () => void;
    filter: (data: any, query_field: QueryField, query_clause: QueryClause) => any[];
    index_filter: (partitions: Partition[], index_name: string, query_clause: QueryClause) => Partition[];
}

// Defines a query field as a string, representing the field to query within data records.
type QueryField = string;

// Enumerates the possible query functions that can be used in a query clause.
type QueryFunction = '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin';

// Represents the structure for a query clause, mapping query functions to their corresponding values.
type QueryClause = {
    [key in QueryFunction]?: any;
}

// Describes a loose query object that can have any string as a key and any type as a value.
type LooseQuery = {
    [key: string]: any;
}

// Represents a more specific query structure where keys are tied to either a specific value or a query clause.
type Query = {
    [key: string]: number | string | QueryClause;
}

export {
    PartitionIndex,
    Partition,
    Table,
    QueryField,
    QueryFunction,
    QueryClause,
    LooseQuery,
    Query
}