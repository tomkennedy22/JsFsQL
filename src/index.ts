import fs from "fs/promises";
import path from "path";

const partition_name_from_partition_index = (partition_index: PartitionIndex) => {
    return Object.entries(partition_index).map((partition_entry: [any, any]) => {
        return `${partition_entry[0]}_${partition_entry[1]}`;
    }).join('_');
}

type PartitionIndex = { [key: string]: any };

type Partition = {
    partition_name: string;
    partition_indices: PartitionIndex;
    storage_location: string;
    data: { [key: string]: any };
    table_folder_path: string;
    primary_key: string;
    is_dirty: boolean;

    insert: (data: any[] | any) => void;
    write_to_file: () => Promise<any>;
}

export class partition implements Partition {
    partition_name: string;
    partition_indices: PartitionIndex;
    storage_location: string;
    data: { [key: string]: any };
    table_folder_path: string;
    primary_key: string;

    is_dirty: boolean;

    constructor({ table_folder_path, partition_indices, primary_key }: { table_folder_path: string, partition_indices: PartitionIndex, primary_key: string }) {
        this.partition_indices = partition_indices;
        this.table_folder_path = table_folder_path;
        this.primary_key = primary_key;
        this.is_dirty = true;
        this.data = {}

        this.partition_name = partition_name_from_partition_index(this.partition_indices)
        this.storage_location = `${table_folder_path}/${this.partition_name}.json`;
    }

    insert(data: any[] | any) {

        let data_to_insert = [];
        if (Array.isArray(data)) {
            data_to_insert = data;
        }
        else {
            data_to_insert = [data];
        }

        for (let row in data_to_insert) {
            let row_pk = data_to_insert[row][this.primary_key];
            this.data[row_pk] = data_to_insert[row];
        }

        this.is_dirty = true;
    }

    write_to_file = async () => {

        if (!this.is_dirty) {
            return;
        }
        this.is_dirty = false;
        let data = JSON.stringify(this, null, 2);

        const dirname = path.dirname(this.storage_location);
        await fs.mkdir(dirname, { recursive: true });

        return fs.writeFile(this.storage_location, data);
    }

}


export type Table = {
    table_name: string;
    indices: string[];
    storage_location: string;
    primary_key: string;

    partitions_by_partition_name: { [key: string]: Partition };
    partition_name_by_primary_key: { [key: string]: string };

    insert: (data: any[]) => void;
    find: (query: any) => any[];
    findOne: (query: any) => any;
    output_to_file: () => void;
    filter: (data: any, query_field: any, query_clause: any) => any;
    index_filter: (partitions: Partition[], index_name: string, query_clause: QueryClause) => Partition[];
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
    storage_location: string;
    output_file_path: string;
    primary_key: string;
    partitions_by_partition_name: { [key: string]: Partition };
    partition_name_by_primary_key: { [key: string]: string };

    constructor({ table_name, indices, folder_path, primary_key }: { table_name: string, indices: string[], folder_path: string, dbname: string, primary_key: string }) {
        this.table_name = table_name;
        this.indices = indices;
        this.primary_key = primary_key;

        this.partitions_by_partition_name = {};
        this.partition_name_by_primary_key = {};

        this.storage_location = `${folder_path}/${table_name}`;
        this.output_file_path = `${this.storage_location}/_${table_name}.json`;
    }


    output_to_file = async () => {
        let partitions = this.find_partitions();

        let output_data = {
            table_name: this.table_name,
            indices: this.indices,
            primary_key: this.primary_key,
            partition_names: Object.keys(this.partitions_by_partition_name),
            output_file_path: this.output_file_path,
            storage_location: this.storage_location,
        }
        let data = JSON.stringify(output_data, null, 2);

        return Promise.all([partitions.map(partition => partition.write_to_file()), fs.writeFile(this.output_file_path, data)]);
    }

    insert(data: any[]) {
        for (let row of data) {
            let row_pk = row[this.primary_key];
            // TODO check for moving partitions
            let partitions_by_partition_name = this.partitions_by_partition_name;
            let partition_indices: PartitionIndex = {}

            this.indices.forEach(index_name => {
                let index_value = row[index_name];
                partition_indices[index_name] = index_value;
            })

            let partition_name = partition_name_from_partition_index(partition_indices)
            this.partition_name_by_primary_key[row_pk] = partition_name;

            if (!this.partitions_by_partition_name.hasOwnProperty(partition_name)) {
                this.partitions_by_partition_name[partition_name] = new partition({ table_folder_path: this.storage_location, partition_indices, primary_key: this.primary_key });
            }

            this.partitions_by_partition_name[partition_name].insert(row);
        }
    }


    find_partitions(): Partition[] {
        return Object.values(this.partitions_by_partition_name);
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
                    if (!(row[query_field] == query_function_value)) {
                        return false;
                    }
                }
            }

            return true;
        });

        return data;
    }

    index_filter(partitions: Partition[], index_name: string, query_clause: QueryClause) {

        console.log('index_filter', { query_clause, index_name })

        Object.keys(query_clause).forEach(function (query_function) {
            let query_function_value = query_clause[query_function as QueryFunction];
            if (query_function == '$eq') {
                partitions = partitions.filter((partition: any) => {
                    return partition.partition_indices[index_name] == query_function_value
                })
            }
        })

        return partitions;
    }


    find(input_query: LooseQuery): any[] {

        if(input_query.hasOwnProperty('$or')) {
            let results = [];
            for (let subquery of input_query['$or']) {
                results.push(this.find(subquery));
            }

            return results.flat();
        }
        
        let query = this.normalize_query(input_query);
        let valid_partitions = this.find_partitions();

        // TODO - check if query has primary key, if so, use that to filter partitions
        // if (query[this.primary_key]) {

        // }

        for (let index_name of this.indices) {
            if (query[index_name]) {
                let query_clause = query[index_name] as QueryClause;
                console.log('Has clause for index', { index_name, query_clause })
                valid_partitions = this.index_filter(valid_partitions, index_name, query_clause);

                delete query[index_name];
            }
        }

        let rows = valid_partitions.map((partition: Partition) => Object.values(partition.data)).flat() || [];
        for (let query_key in query) {
            let query_clause = query[query_key] as QueryClause;
            rows = this.filter(rows, query_key, query_clause);
        }

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
}


export class database {
    dbname: any;
    tables: { [key: string]: Table };
    folder_path: string;
    storage_location: string;
    output_file_path: string;

    constructor({ dbname, folder_path }: { dbname: string, folder_path: string }) {
        this.dbname = dbname;
        this.folder_path = folder_path;
        this.tables = {};

        this.storage_location = `${folder_path}/${dbname}`;
        this.output_file_path = `${this.storage_location}/_${dbname}.json`;
    }

    add_table({ table_name, indices, primary_key }: { table_name: string, indices: string[], primary_key: string }) {
        let new_table = new table({ table_name, indices, folder_path: this.folder_path, dbname: this.dbname, primary_key });
        this.tables[table_name] = new_table;
        return new_table;
    }

    save_database = async () => {
        let tables = Object.values(this.tables);
        let table_info = tables.map(table => ({ table_name: table.table_name, indices: table.indices, primary_key: table.primary_key }));

        let save_data = {
            dbname: this.dbname,
            tables: table_info,
            storage_location: this.storage_location,
            output_file_path: this.output_file_path,
        }

        let data = JSON.stringify(save_data, null, 2);

        return Promise.all([tables.map(table => table.output_to_file()), fs.writeFile(this.output_file_path, data)]);

    }
}