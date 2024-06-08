import fs from "fs/promises";
import path from "path";
import { type_join_criteria, type_join_type, type_loose_query, type_partition, type_partition_index, type_query, type_query_clause, type_table, type_table_init } from "./types";
import { get_from_dict, partition_name_from_partition_index } from "./utils";
import { partition } from "./partition";
import decircular from "decircular";


export class table<T extends object> implements type_table<T> {
    table_name: string;
    indices: string[];
    storage_location: string;
    output_file_path: string;
    primary_key: string;
    proto?: any;
    partitions_by_partition_name: { [key: string]: type_partition };
    partition_name_by_primary_key: { [key: string]: string };
    do_compression: boolean;

    table_connections: { [key: string]: { join_key: string, join_type: type_join_type } };

    delete_key_list: string[];

    constructor({ table_name, indices, storage_location, primary_key, proto, delete_key_list, do_compression }: type_table_init) {
        this.table_name = table_name;
        this.indices = indices || [];
        this.primary_key = primary_key;
        this.proto = proto;

        this.table_connections = {};

        this.partitions_by_partition_name = {};
        this.partition_name_by_primary_key = {};

        this.delete_key_list = delete_key_list || [];
        this.do_compression = do_compression || false;

        this.storage_location = `${storage_location}/${table_name}`;
        this.output_file_path = `${this.storage_location}/_${table_name}.json`;
    }

    output_to_file = async () => {
        try {
            let partitions = this.find_partitions();

            let output_data = {
                table_name: this.table_name,
                indices: this.indices,
                primary_key: this.primary_key,
                partition_names: Object.keys(this.partitions_by_partition_name).sort(),
                output_file_path: this.output_file_path,
                storage_location: this.storage_location,
                table_connections: this.table_connections,
                do_compression: this.do_compression,
                delete_key_list: this.delete_key_list
            }
            let data = JSON.stringify(output_data, null, 2);

            // Ensure the directory exists where the file will be stored
            const dirname = path.dirname(this.output_file_path);
            await fs.mkdir(dirname, { recursive: true });

            // Wait for all partitions to finish writing
            const writePromises = partitions.map(async (partition) => {
                // If the partition is already being written to, wait
                let wait_count = 0;
                let wait_limit = 10;
                let wait_time = 50;
                while (partition.write_lock && wait_count < wait_limit) {
                    // console.log('Waiting for write lock on partition', partition.partition_name, '. Wait count: ', wait_count)
                    wait_time *= 1.5;
                    wait_count += 1;

                    await new Promise(resolve => setTimeout(resolve, wait_time)); // wait for 100ms before checking again
                }

                if (wait_count >= wait_limit) {
                    console.error(`Write lock timeout exceeded for partition ${partition.partition_name} after ${wait_count} attempts`);
                    return;
                }

                // Now write to the partition
                return partition.write_to_file();
            });

            // Wait for all partition writes to complete
            await Promise.all(writePromises);

            // Write the table metadata
            await fs.writeFile(this.output_file_path, data);

        } catch (error) {
            console.error("Error in output_to_file:", error);
            // Handle the error appropriately
            // Perhaps set a flag to indicate that the write operation failed
        }
    }

    read_from_file = async () => {
        // console.trace('Reading from file')
        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);

            let { table_name, indices, primary_key, partition_names, storage_location, table_connections, do_compression, delete_key_list } = parsed_data;

            this.table_name = table_name;
            this.indices = indices;
            this.primary_key = primary_key;
            this.storage_location = storage_location;
            this.table_connections = table_connections;
            this.do_compression = do_compression;
            this.delete_key_list = delete_key_list || [];

            // Collecting promises for each partition read operation
            const partitionReadPromises = partition_names.map(async (partition_name: string) => {
                // let partition_location = `${storage_location}/${partition_name}.json`;
                let partition_indices = {};
                let new_partition = new partition({ storage_location, partition_indices, primary_key, proto: this.proto, do_compression: this.do_compression, partition_name, delete_key_list: this.delete_key_list || [] });
                await new_partition.read_from_file();

                for (let pk in new_partition.data) {
                    this.partition_name_by_primary_key[pk] = partition_name;
                }

                this.partitions_by_partition_name[partition_name] = new_partition;
            });

            // Wait for all partition read operations to complete
            await Promise.all(partitionReadPromises);

            Promise.resolve();
        }
        catch (error) {
            console.log('Error reading from file', this.output_file_path, error)
        }
    }

    /**
     * Inserts data into the index.
     * @param data Array of objects, each representing a row to be inserted, keyed by index fields.
     */
    insert(data: T[] | T) {

        try {
            if (!Array.isArray(data)) {
                data = [data];
            }

            data = this.cleanse_before_alter(data);

            // Process each row for insertion into its partition
            for (let row of data) {

                let row_pk = get_from_dict(row, this.primary_key); // Capture the primary key value from the row

                let partition_indices: type_partition_index = {};
                // Generate partition index keys from the row based on the table indices
                this.indices.forEach(index_name => {
                    partition_indices[index_name] = get_from_dict(row, index_name);
                });

                // Determine the partition name from the indices for row placement
                let partition_name = partition_name_from_partition_index(partition_indices);
                this.partition_name_by_primary_key[row_pk] = partition_name;

                // Create a new partition if it doesn't exist
                if (!this.partitions_by_partition_name.hasOwnProperty(partition_name)) {
                    this.partitions_by_partition_name[partition_name] = new partition({
                        storage_location: this.storage_location,
                        partition_indices,
                        primary_key: this.primary_key,
                        proto: this.proto,
                        do_compression: this.do_compression,
                        delete_key_list: this.delete_key_list || [],
                    });
                }

                // Delegate the row insertion to the partition's own insert method
                this.partitions_by_partition_name[partition_name].insert(row);
            }

            return true;
        }
        catch (error) {
            console.error('Error inserting data into table:', error);
            return false;
        }

    }

    update(data: T[] | T): void {
        if (!Array.isArray(data)) {
            data = [data];
        }

        data = this.cleanse_before_alter(data);

        for (const row of data) {
            const rowPk = get_from_dict(row, this.primary_key);

            let existing_partition_name = this.partition_name_by_primary_key[rowPk];
            if (!existing_partition_name) {
                throw new Error(`Row with primary key ${rowPk} does not exist and cannot be updated.`);
            }

            this.partitions_by_partition_name[existing_partition_name].is_dirty = true;

            // Remove the existing row from the partition
            delete this.partitions_by_partition_name[existing_partition_name].data[rowPk];
            delete this.partition_name_by_primary_key[rowPk];

            this.insert(row);
        }
    }


    cleanse_before_alter(data: T[]): T[] {

        let new_list = data.map((item) => {
            let delete_list = this.delete_key_list || [];
            if (!delete_list) {
                return item;
            }

            for (let delete_key of delete_list) {
                // @ts-ignore
                delete item[delete_key];
            }
            return item;
        })
        return new_list;
    }


    /**
     * Returns an array of all partitions in the index.
     * @returns {type_partition[]} An array of Partition objects.
     */
    find_partitions(): type_partition[] {
        return Object.values(this.partitions_by_partition_name);
    }


    // TODO - clean queries, so stuff like IN changes to a SET
    /**
     * Normalizes a given query by converting any string or number values to an object with an $eq operator.
     * @param query - The query to normalize.
     * @returns The normalized query.
     */
    normalize_query(query: type_loose_query) {

        for (let query_field in query) {
            let query_clause: type_query_clause = query[query_field] as type_query_clause;
            if (typeof query_clause === 'string' || typeof query_clause === 'number' || typeof query_clause === 'boolean') {
                query[query_field] = { $eq: query_clause };
            }
        }

        return query as type_query;
    }

    /**
     * Filters data based on a query field and query clause.
     * @param data Array of objects to filter.
     * @param query_field The field of the objects to query against.
     * @param query_clause Object defining the query operations and values.
     * @returns Filtered data array.
     */
    filter(data: T[], query_field: string, query_clause: type_query_clause): T[] {
        if (!query_clause || Object.keys(query_clause).length === 0) return data;
        return data.filter((row: T) => {
            // console.log('In filter', { row, query_field, query_clause })
            for (const [query_function, query_value] of Object.entries(query_clause)) {
                switch (query_function) {
                    case '$eq':
                        if (get_from_dict(row, query_field) !== query_value) return false;
                        break;
                    case '$ne':
                        if (get_from_dict(row, query_field) === query_value) return false;
                        break;
                    case '$gt':
                        if (!(get_from_dict(row, query_field) > query_value)) return false;
                        break;
                    case '$gte':
                        if (!(get_from_dict(row, query_field) >= query_value)) return false;
                        break;
                    case '$lt':
                        if (!(get_from_dict(row, query_field) < query_value)) return false;
                        break;
                    case '$lte':
                        if (!(get_from_dict(row, query_field) <= query_value)) return false;
                        break;
                    case '$between':
                        if (!(get_from_dict(row, query_field) >= query_value[0]) || (!get_from_dict(row, query_field) <= query_value[1])) return false;
                        break;
                    case '$in':
                        if (!query_value.includes(get_from_dict(row, query_field))) return false;
                        break;
                    case '$nin':
                        if (query_value.includes(get_from_dict(row, query_field))) return false;
                        break; default:
                        throw new Error(`Unsupported query function: ${query_function}`);
                }
            }
            return true; // Row passes all query clauses
        });
    }


    /**
     * Filters partitions based on a given index name and query clause.
     * @param partitions Array of Partition objects to filter.
     * @param index_name The index field to be queried against.
     * @param query_clause Object defining the query operations and values.
     * @returns Filtered array of Partition objects.
     */
    index_partition_filter(partitions: type_partition[], index_name: string, query_clause: type_query_clause): type_partition[] {
        let filteredPartitions = partitions;

        for (const [query_function, query_value] of Object.entries(query_clause)) {
            switch (query_function) {
                case '$eq':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] === query_value);
                    break;
                case '$ne':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] !== query_value);
                    break;
                case '$gt':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] > query_value);
                    break;
                case '$gte':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] >= query_value);
                    break;
                case '$lt':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] < query_value);
                    break;
                case '$lte':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] <= query_value);
                    break;
                case '$between':
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_indices[index_name] >= query_value[0] && partition.partition_indices[index_name] <= query_value[1]);
                    break;
                case '$in':
                    filteredPartitions = filteredPartitions.filter(partition => query_value.includes(partition.partition_indices[index_name]));
                    break;
                case '$nin':
                    filteredPartitions = filteredPartitions.filter(partition => !query_value.includes(partition.partition_indices[index_name]));
                    break;
                default:
                    throw new Error(`Unsupported query function: ${query_function}`);
            }

            // If there are no partitions left after filtering with a query function, break early
            if (filteredPartitions.length === 0) break;
        }

        return filteredPartitions;
    }


    primary_key_partition_filter(partitions: type_partition[], query_clause: type_query_clause): type_partition[] {
        let filteredPartitions = partitions;

        let primary_key_list, gt_primary_key_list, partition_name_set: Set<string>;

        for (const [query_function, query_value] of Object.entries(query_clause)) {
            switch (query_function) {
                case '$eq':
                    let chosen_partition = this.partitions_by_partition_name[this.partition_name_by_primary_key[query_value]]
                    filteredPartitions = chosen_partition ? [chosen_partition] : [];
                    break;
                case '$ne':
                    let partition_name = this.partition_name_by_primary_key[query_value];
                    filteredPartitions = filteredPartitions.filter(partition => partition.partition_name !== partition_name);
                    break;
                case '$gt':
                    primary_key_list = Object.keys(this.partition_name_by_primary_key);
                    gt_primary_key_list = primary_key_list.filter(pk => pk > query_value);
                    partition_name_set = new Set(gt_primary_key_list.map(pk => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$gte':
                    primary_key_list = Object.keys(this.partition_name_by_primary_key);
                    gt_primary_key_list = primary_key_list.filter(pk => pk >= query_value);
                    partition_name_set = new Set(gt_primary_key_list.map(pk => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$lt':
                    primary_key_list = Object.keys(this.partition_name_by_primary_key);
                    gt_primary_key_list = primary_key_list.filter(pk => pk < query_value);
                    partition_name_set = new Set(gt_primary_key_list.map(pk => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$lte':
                    primary_key_list = Object.keys(this.partition_name_by_primary_key);
                    gt_primary_key_list = primary_key_list.filter(pk => pk <= query_value);
                    partition_name_set = new Set(gt_primary_key_list.map(pk => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$between':
                    primary_key_list = Object.keys(this.partition_name_by_primary_key);
                    gt_primary_key_list = primary_key_list.filter(pk => pk >= query_value[0] && pk <= query_value[1]);
                    partition_name_set = new Set(gt_primary_key_list.map(pk => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$in':
                    partition_name_set = new Set(query_value.map((pk: any) => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => partition_name_set.has(partition.partition_name));
                    break;
                case '$nin':
                    partition_name_set = new Set(query_value.map((pk: any) => this.partition_name_by_primary_key[pk]));
                    filteredPartitions = filteredPartitions.filter(partition => !partition_name_set.has(partition.partition_name));
                    break;
                default:
                    throw new Error(`Unsupported query function: ${query_function}`);
            }

            // If there are no partitions left after filtering with a query function, break early
            if (filteredPartitions.length === 0) break;
        }

        return filteredPartitions;
    }

    delete = async (query?: type_loose_query) => {

        if (!query) {
            await this.clear();
            return Promise.resolve();
        }

        let rows = this.find(query);

        const delete_row = (row: T) => {
            let row_pk = get_from_dict(row, this.primary_key);
            let partition_name = this.partition_name_by_primary_key[row_pk];
            let partition = this.partitions_by_partition_name[partition_name];

            partition.is_dirty = true;

            delete partition.data[row_pk];
            delete this.partition_name_by_primary_key[row_pk];
        }


        if (Array.isArray(rows)) {
            for (let row of rows) {
                delete_row(row);
            }
        }
        else {
            delete_row(rows);
        }


        return Promise.resolve();
    }

    clear = async () => {

        for (let name in this.partitions_by_partition_name) {
            let partition = this.partitions_by_partition_name[name];
            await partition.delete_file();
        }

        this.partitions_by_partition_name = {};
        this.partition_name_by_primary_key = {};

        return Promise.resolve();
    }

    // TODO - this assumes the primary key is an integer. Need to fix this.
    next_id = () => {
        let keys = Object.keys(this.partition_name_by_primary_key);
        if (keys.length === 0) {
            return 1;
        }
        let max_id = Math.max(...keys.map(key => parseInt(key)));
        return max_id + 1;
    }

    count(): number {
        return Object.keys(this.partition_name_by_primary_key).length;
    }

    find(input_query?: type_loose_query): T | T[] {

        if (!input_query) {
            return Object.values(this.partitions_by_partition_name).map(partition => Object.values(partition.data)).flat();
        }

        if (input_query.hasOwnProperty('$or')) {
            let combined_set = [];
            for (let subquery of input_query['$or']) {
                let subquery_rows = this.find(subquery);

                if (Array.isArray(subquery_rows)) {
                    combined_set.push(...subquery_rows);
                }
                else {
                    combined_set.push(subquery_rows);
                }
            }

            return decircular(combined_set);
        }

        let query = this.normalize_query(input_query);
        let valid_partitions = this.find_partitions();

        if (query[this.primary_key]) {
            let query_clause = query[this.primary_key] as type_query_clause;
            valid_partitions = this.primary_key_partition_filter(valid_partitions, query_clause);
        }

        for (let index_name of this.indices) {
            if (query[index_name]) {
                let query_clause = query[index_name] as type_query_clause;
                valid_partitions = this.index_partition_filter(valid_partitions, index_name, query_clause);
            }
        }


        // TODO better key filtering

        let rows = valid_partitions.map((partition: type_partition) => Object.values(partition.data)).flat() || [];

        for (let query_key in query) {
            let query_clause = query[query_key] as type_query_clause;
            rows = this.filter(rows, query_key, query_clause);
        }

        return decircular(rows);
    }

    findOne(query?: type_loose_query): T | null {

        let data = this.find(query);

        if (!Array.isArray(data)) {
            return data;
        }
        if (data.length == 0) {
            return null;
        }
        else {
            return data[0];
        }
    }

    get_table_connection(foreign_table_name: string): type_join_criteria | null {
        if (!this.table_connections) {
            return null;
        }
        return this.table_connections[foreign_table_name];
    }

    get_all_foreign_keys(): string[] {
        if (!this.table_connections) {
            return [];
        }

        return Object.values(this.table_connections).map(connection => connection.join_key);
    }

    get_foreign_keys_and_primary_keys(): string[] {
        return [...(new Set([...this.get_all_foreign_keys(), this.primary_key]))];
    }
}