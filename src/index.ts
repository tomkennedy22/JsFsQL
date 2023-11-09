import fs from "fs/promises";
import path from "path";

type Partition = {
    partition_name: string;
    partition_indices: PartitionIndex[];
    storage_location: string;
    data: any[];
    table_folder_path: string;
    insert: (data: any[] | any) => void;
    write_to_file: () => Promise<any>;
}

type PartitionIndex = { index_name: string, index_value: any };

export class partition implements Partition {
    partition_name: string;
    partition_indices: PartitionIndex[];
    storage_location: string;
    data: any[];
    table_folder_path: string;

    constructor({ table_folder_path, partition_indices }: { table_folder_path: string, partition_indices: PartitionIndex[] }) {
        this.partition_indices = partition_indices;
        this.table_folder_path = table_folder_path;
        this.data = []

        this.partition_name = partition_indices.map((partition_index: PartitionIndex) => {
            return `${partition_index.index_name}_${partition_index.index_value}`;
        }).join('_');
        this.storage_location = `${table_folder_path}/${this.partition_name}.json`;
    }

    insert(data: any[] | any) {

        if (Array.isArray(data)) {
            this.data.push(...data);
        }
        else {
            this.data.push(data);
        }
    }

    write_to_file = async () => {
        // Stringify the data with an indentation of 2 spaces
        let data = JSON.stringify(this, null, 2);

        // Create the directory if it does not exist
        const dirname = path.dirname(this.storage_location);
        await fs.mkdir(dirname, { recursive: true });

        // Write the file with pretty printed JSON
        return fs.writeFile(this.storage_location, data);
    }

}


export type Table = {
    table_name: string;
    indices: string[];
    indexed_data: any;
    storage_location: string;
    primary_key: string;

    partitions_by_partition_name: { [key: string]: Partition };

    insert: (data: any[]) => void;
    find: (query: any) => any[];
    findOne: (query: any) => any;
    output_to_file: () => void;
    filter: (data: any, query_field:any, query_clause: any) => any;
    index_filter: (data: any, query_clause: any) => any;
}

type QueryField = string;

type QueryFunction = '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin';

type QueryClause = {
    [QueryField in QueryFunction]?: any;
}

type LooseQuery = {
    [key: string]: any;
}

export type Query = {
    [key: string]: number | string | QueryClause;
}


export class table implements Table {
    table_name: string;
    indices: string[];
    indexed_data: { [key: string]: any };
    storage_location: string;
    primary_key: string;
    partitions_by_partition_name: { [key: string]: Partition };

    constructor({ table_name, indices, folder_path, dbname, primary_key }: { table_name: string, indices: string[], folder_path: string, dbname: string, primary_key: string }) {
        this.table_name = table_name;
        this.indices = indices;
        this.primary_key = primary_key;

        this.indexed_data = {};
        this.partitions_by_partition_name = {};

        this.storage_location = `${folder_path}/${dbname}/${table_name}`;
    }

    insert(data: any[]) {
        for (let row of data) {
            let row_pk = row[this.primary_key];
            // TODO check for moving partitions
            let navigated_index = this.indexed_data;
            let partition_indices: PartitionIndex[] = []

            this.indices.forEach((index_name, idx) => {
                let is_last_index = idx == this.indices.length - 1;
                let row_index_value = row[index_name];
                let index_in_navigation_index = navigated_index[row_index_value] != undefined;

                partition_indices.push({ index_name, index_value: row_index_value });



                if (!index_in_navigation_index) {
                    if (!is_last_index) {
                        navigated_index[row_index_value] = {};
                        navigated_index = navigated_index[row_index_value];
                    }
                    else if (is_last_index) {
                        navigated_index[row_index_value] = new partition({ table_folder_path: this.storage_location, partition_indices });
                        navigated_index[row_index_value].insert(row)
                    }
                }
                else {
                    if (is_last_index) {
                        navigated_index[row_index_value].insert(row);
                    }
                    else if (!is_last_index) {
                        navigated_index = navigated_index[row_index_value];
                    }
                }

            })
        }
    }

    output_to_file = async () => {
        let partitions = this.find_partitions(this.indexed_data);

        return Promise.all(partitions.map(partition => partition.write_to_file()));
    }


    find_partitions(data: any): Partition[] {
        // Base case: if data is a partition, return it in an array
        if (data instanceof partition) {
            return [data]; // return as an array
        }

        // If data is an object (and not null, which also returns "object" for typeof), iterate its keys
        if (data && typeof data === 'object' && !(data instanceof Array)) {
            let partitions: Partition[] = [];
            for (const key in data) {
                if (data.hasOwnProperty(key)) { // Check if the key is actually a property of 'data'
                    const result = this.find_partitions(data[key]); // Recursive call
                    partitions = partitions.concat(result); // Concatenate results
                }
            }
            return partitions; // Return the cumulative partitions
        }

        // If data is an array, loop through its elements and find partitions
        if (Array.isArray(data)) {
            let partitions: Partition[] = [];
            for (const item of data) {
                const result = this.find_partitions(item); // Recursive call
                partitions = partitions.concat(result); // Concatenate results
            }
            return partitions; // Return the cumulative partitions
        }

        // If it's neither an object nor an array, return an empty array
        return [];
    }

    // TODO - clean queries, so stuff like IN changes to a SET
    normalize_query(query: LooseQuery) {

        for (let query_field in query) {
            let query_clause: QueryClause = query[query_field] as QueryClause;
            if (typeof query_clause === 'string' || typeof query_clause === 'number') {
                query[query_field] = { $eq: query_clause };
            }
        }

        return query as Query;
    }

    filter(data: any, query_field: string, query_clause: QueryClause) {

        data = data.filter((row: any) => {
            for (let query_function in query_clause) {
                let query_function_value = query_clause[query_function as QueryFunction];
                if (query_function == '$eq') {
                    if (!(row[query_field] == query_function_value)){
                        return false;
                    }
                }
            }

            return true;
        });

        return data;
    }

    index_filter(data: any, query_clause: QueryClause) {

        Object.keys(query_clause).forEach(function(query_function) {
            let query_function_value = query_clause[query_function as QueryFunction];
            console.log('index_filter loop', {query_function, query_function_value, data, query_clause})
            if (query_function == '$eq') {
                data = data[query_function_value];
                console.log('index_filter $eq', {data})
            }
        })

        return data;
    }

    merge_indexed_data(data: any[]) {
        let merged_data: any = {};
        for (let values of data) {
            for(let key in values) {
                if (!merged_data.hasOwnProperty(key)) {
                    merged_data[key] = values[key];
                }
            }
        }

        console.log('merge_indexed_data', {merged_data})

        return merged_data;
    }


    find(input_query: LooseQuery) {

        let query = this.normalize_query(input_query);

        console.log({ query })
        let filtered_indexed_data = this.indexed_data;

        if (query[this.primary_key]) {

        }

        for (let index_name of this.indices) {
            if (query[index_name]){
                let query_clause = query[index_name] as QueryClause;
                console.log('Has clause for index', {index_name, query_clause})
                filtered_indexed_data = this.index_filter(filtered_indexed_data, query_clause);

                delete query[index_name];
            }
            else {
                console.log('No clause for index', {index_name, filtered_indexed_data}, )
                filtered_indexed_data = this.merge_indexed_data(Object.values(filtered_indexed_data))
            }
        }

        let partitions = this.find_partitions(filtered_indexed_data);
        let rows = partitions.map(partition => partition.data).flat() || [];
        for (let query_key in query) {
            let query_clause = query[query_key] as QueryClause;
            rows = this.filter(rows, query_key, query_clause);
        }

        console.log({rows, query})

        rows = rows.map((row: any) => row.name);

        return rows;
    }

    findOne(query: Query) {

        let data = this.find(query);
        if (data.length == 0) {
            return null;
        }
        else {
            return data[0];
        }
    }

    // get_data_flat() {
    //     return this.data;
    // }

}


export class database {
    dbname: any;
    tables: { [key: string]: Table };
    folder_path: string;

    constructor({ dbname, folder_path }: { dbname: string, folder_path: string }) {
        this.dbname = dbname;
        this.folder_path = folder_path;
        this.tables = {};
    }

    add_table({ table_name, indices, primary_key }: { table_name: string, indices: string[], primary_key: string }) {
        let new_table = new table({ table_name, indices, folder_path: this.folder_path, dbname: this.dbname, primary_key });
        this.tables[table_name] = new_table;
        return new_table;
    }
}