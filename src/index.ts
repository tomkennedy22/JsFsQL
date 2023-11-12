import fs from "fs/promises";
import path from "path";
import { type_database, type_loose_query, type_partition, type_partition_index, type_query, type_query_clause, type_table } from "./types";

// Helper function to generate a partition name based on the partition index.
// It converts the index into a string format suitable for file naming.
const partition_name_from_partition_index = (partition_index: type_partition_index): string => {
    return Object.entries(partition_index)
        .map(([indexKey, indexValue]) => `${indexKey}_${indexValue}`)
        .join('_');
}



// The Partition class definition, implementing the Partition type.
export class partition implements type_partition {
    partition_name: string;
    partition_indices: type_partition_index;
    storage_location: string;
    proto: any;
    output_file_path: string;
    data: { [key: string]: any };
    primary_key: string;
    is_dirty: boolean = true; // Default is_dirty to true to indicate the partition requires saving upon creation.

    // Constructor to initialize a new partition with given properties.
    constructor({ storage_location, partition_indices, primary_key, proto }: { storage_location: string, partition_indices: type_partition_index, primary_key: string, proto: any }) {
        this.partition_indices = partition_indices;
        this.primary_key = primary_key;
        this.proto = proto;
        this.data = {}; // Initialize data as an empty object.
        // Generate a partition name from the provided indices and form the storage location path.
        this.partition_name = partition_name_from_partition_index(partition_indices);

        this.storage_location = storage_location;
        this.output_file_path = `${storage_location}/${this.partition_name}.json`; // Storage location is derived from the table folder path and partition name.
    }

    /**
     * Inserts one or multiple new rows of data into the dataset.
     * The function normalizes single objects to arrays for unified processing.
     * It also flags the dataset as 'dirty' to indicate that changes have been made since the last save or update.
     * @param {any[] | any} data - The new data to be inserted, either a single object or an array of objects.
     */
    insert(data: any[] | any): void {
        // Normalize data into an array for unified processing
        const dataToInsert = Array.isArray(data) ? data : [data];

        // Insert each row into the dataset using its primary key for identification
        dataToInsert.forEach((row) => {
            const rowPk = row[this.primary_key];
            if (rowPk === undefined) {
                throw new Error(`Primary key value missing in the data row. Cannot insert into partition. Table ${this.partition_name} and primary key ${this.primary_key} and value ${rowPk}`);
            }
            else if (this.data.hasOwnProperty(rowPk)) {
                throw new Error(`Duplicate primary key value: ${rowPk} for field ${this.primary_key} in partition ${this.partition_name}`);
            }
            this.data[rowPk] = row;

            // Mark the dataset as 'dirty' to indicate that the state has changed   
            this.is_dirty = true;
        });

    }

    update(row: any): void {
        const rowPk = row[this.primary_key];
        if (!this.data.hasOwnProperty(rowPk)) {
            throw new Error(`Row with primary key ${rowPk} does not exist in partition ${this.partition_name}.`);
        }

        // Update the row and mark partition as dirty
        this.data[rowPk] = row;
        this.is_dirty = true;
    }


    /**
     * Asynchronously writes the current state of the object to a file in JSON format.
     * The write operation is performed only if changes have been made to the object (indicated by the 'is_dirty' flag).
     * The 'is_dirty' flag is reset to 'false' before writing to prevent redundant saves.
     */
    write_to_file = async (): Promise<void> => {
        // Skip writing to file if no changes have been made
        if (!this.is_dirty) {
            return;
        }

        // Reset the 'is_dirty' flag to indicate the state is being saved
        this.is_dirty = false;

        // Serialize the object to a JSON string with pretty-printing
        let data = JSON.stringify(this, null, 2);

        // Ensure the directory exists where the file will be stored
        const dirname = path.dirname(this.output_file_path);
        await fs.mkdir(dirname, { recursive: true });

        // Write the serialized data to the file at the specified storage location
        return fs.writeFile(this.output_file_path, data);
    }

    read_from_file = async () => {
        console.trace('Reading from file')
        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);

            let { partition_name, partition_indices, data: partition_data, storage_location, primary_key } = parsed_data;

            console.log({ partition_name, partition_indices, partition_data, storage_location, primary_key })

            this.partition_name = partition_name;
            this.partition_indices = partition_indices;
            this.data = Object.fromEntries(
                Object.entries(partition_data).map(([key, value]) => [key, new this.proto(value)])
            );
            this.storage_location = storage_location;
            this.primary_key = primary_key;

            return Promise.resolve();
        }
        catch (error) {
            console.log('Error reading from file', error, this.output_file_path)
        }
    }

    delete_file = async () => {
        try {
            this.data = {};

            // Delete the file associated with this partition
            await fs.unlink(this.output_file_path);
            console.log(`Deleted file at: ${this.output_file_path}`);
        } catch (error) {
            // Handle possible errors, such as file not existing
            console.error(`Error deleting file at: ${this.output_file_path}`, error);
        }

        return Promise.resolve();
    }
}



export class table implements type_table {
    table_name: string;
    indices: string[];
    storage_location: string;
    output_file_path: string;
    primary_key: string;
    proto?: any;
    partitions_by_partition_name: { [key: string]: type_partition };
    partition_name_by_primary_key: { [key: string]: string };

    constructor({ table_name, indices, storage_location, primary_key, proto }: { table_name: string, indices: string[], storage_location: string, dbname: string, primary_key: string, proto?: any }) {
        this.table_name = table_name;
        this.indices = indices || [];
        this.primary_key = primary_key;
        this.proto = proto;

        this.partitions_by_partition_name = {};
        this.partition_name_by_primary_key = {};

        this.storage_location = `${storage_location}/${table_name}`;
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

        // Ensure the directory exists where the file will be stored
        const dirname = path.dirname(this.output_file_path);
        await fs.mkdir(dirname, { recursive: true });

        await Promise.all([partitions.map(partition => partition.write_to_file())]);
        await fs.writeFile(this.output_file_path, data)
    }

    read_from_file = async () => {
        console.trace('Reading from file')
        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);
    
            let { table_name, indices, primary_key, partition_names, storage_location } = parsed_data;
    
            console.log({ table_name, indices, primary_key, partition_names, storage_location })
    
            this.table_name = table_name;
            this.indices = indices;
            this.primary_key = primary_key;
            this.storage_location = storage_location;
    
            // Collecting promises for each partition read operation
            const partitionReadPromises = partition_names.map(async (partition_name: string) => {
                console.log('Reading partition', { partition_name });
                let partition_data = await fs.readFile(`${storage_location}/${partition_name}.json`, 'utf-8');
                let parsed_partition_data = JSON.parse(partition_data);
                let { partition_indices, data } = parsed_partition_data;
    
                // Create and assign partition instance
                let new_partition = new partition({ storage_location, partition_indices, primary_key, proto:this.proto });
                new_partition.data = data;
                new_partition.is_dirty = false;
    
                this.partitions_by_partition_name[partition_name] = new_partition;
    
                // Process primary keys if necessary
                for (let pk in data) {
                    this.partition_name_by_primary_key[pk] = partition_name;
                }
            });
    
            // Wait for all partition read operations to complete
            await Promise.all(partitionReadPromises);
    
            console.log('All partitions have been read from file');
    
            Promise.resolve();
    
        }
        catch (error) {
            console.log('Error reading from file', error, this.output_file_path)
        }
    }

    /**
     * Inserts data into the index.
     * @param data Array of objects, each representing a row to be inserted, keyed by index fields.
     */
    insert(data: any[]) {
        // Process each row for insertion into its partition
        for (let row of data) {
            let row_pk = row[this.primary_key]; // Capture the primary key value from the row

            let partition_indices: type_partition_index = {};
            console.log('In insert', { row, indices: this.indices })
            // Generate partition index keys from the row based on the table indices
            this.indices.forEach(index_name => {
                partition_indices[index_name] = row[index_name];
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
                });
            }

            // Delegate the row insertion to the partition's own insert method
            this.partitions_by_partition_name[partition_name].insert(row);
        }
    }

    update(data: any[] | any): void {
        if (!Array.isArray(data)) {
            data = [data];
        }

        for (const row of data) {
            // Retrieve primary key value from the row
            const rowPk = row[this.primary_key];
            if (rowPk === undefined) {
                throw new Error(`Primary key value missing in the data row. Cannot update.`);
            }

            // Find partition name using the primary key
            const partitionName = this.partition_name_by_primary_key[rowPk];
            if (!partitionName) {
                throw new Error(`Row with primary key ${rowPk} does not exist and cannot be updated.`);
            }

            // Retrieve the corresponding partition
            const partition = this.partitions_by_partition_name[partitionName];
            if (!partition) {
                throw new Error(`Partition ${partitionName} does not exist. Cannot update row with primary key ${rowPk}.`);
            }

            // Update the row within the partition if it exists
            partition.update(row);
        }
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
            if (typeof query_clause === 'string' || typeof query_clause === 'number') {
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
    filter(data: any[], query_field: string, query_clause: type_query_clause): any[] {
        return data.filter((row: any) => {
            for (const [query_function, query_value] of Object.entries(query_clause)) {
                switch (query_function) {
                    case '$eq':
                        if (row[query_field] !== query_value) return false;
                        break;
                    case '$ne':
                        if (row[query_field] === query_value) return false;
                        break;
                    case '$gt':
                        if (!(row[query_field] > query_value)) return false;
                        break;
                    case '$gte':
                        if (!(row[query_field] >= query_value)) return false;
                        break;
                    case '$lt':
                        if (!(row[query_field] < query_value)) return false;
                        break;
                    case '$lte':
                        if (!(row[query_field] <= query_value)) return false;
                        break;
                    case '$in':
                        if (!query_value.includes(row[query_field])) return false;
                        break;
                    case '$nin':
                        if (query_value.includes(row[query_field])) return false;
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
    index_filter(partitions: type_partition[], index_name: string, query_clause: type_query_clause): type_partition[] {
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

    clear = async () => {

        for (let name in this.partitions_by_partition_name) {
            let partition = this.partitions_by_partition_name[name];
            await partition.delete_file();
        }

        this.partitions_by_partition_name = {};
        this.partition_name_by_primary_key = {};

        return Promise.resolve();
    }

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

    find(input_query?: type_loose_query): any[] {

        if (!input_query) {
            return Object.values(this.partitions_by_partition_name).map(partition => Object.values(partition.data)).flat();
        }

        if (input_query.hasOwnProperty('$or')) {
            let results = [];
            for (let subquery of input_query['$or']) {
                results.push(this.find(subquery));
            }

            return results.flat();
        }

        let query = this.normalize_query(input_query);
        let valid_partitions = this.find_partitions();

        if (query[this.primary_key]) {
            let query_clause = query[this.primary_key] as type_query_clause;
            valid_partitions = valid_partitions.filter(partition => partition.partition_name === this.partition_name_by_primary_key[query_clause['$eq']]);
        }

        console.log('Valid partitions', { valid_partitions })

        for (let index_name of this.indices) {
            if (query[index_name]) {
                let query_clause = query[index_name] as type_query_clause;
                console.log('Has clause for index', { index_name, query_clause })
                valid_partitions = this.index_filter(valid_partitions, index_name, query_clause);

                delete query[index_name];
            }
        }


        // TODO better key filtering
        // let rows;

        // if (query[this.primary_key]) {
        //     let query_clause = query[this.primary_key] as type_query_clause;
        //     valid_partitions = valid_partitions.filter(partition => partition.partition_name === this.partition_name_by_primary_key[query_clause['$eq']]);
        // }

        let rows = valid_partitions.map((partition: type_partition) => Object.values(partition.data)).flat() || [];

        for (let query_key in query) {
            let query_clause = query[query_key] as type_query_clause;
            rows = this.filter(rows, query_key, query_clause);
        }

        return rows;
    }

    findOne(query?: type_loose_query) {

        let data = this.find(query);
        if (data.length == 0) {
            return null;
        }
        else {
            return data[0];
        }
    }
}


export class database implements type_database {
    dbname: string;
    tables: { [key: string]: type_table };
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

    add_table({ table_name, indices, primary_key, proto }: { table_name: string, indices: string[], primary_key: string, proto?:any }): type_table {
        console.log('In add table', { table_name, indices, primary_key, tables: this.tables })

        if (!table_name) {
            throw new Error('Table name is required');
        }

        if (!this.tables.hasOwnProperty(table_name)) {
            let new_table = new table({ table_name, indices, storage_location: this.storage_location, dbname: this.dbname, primary_key, proto });
            this.tables[table_name] = new_table;
            console.log('In add table', { new_table, tables: this.tables  })
            return new_table as type_table;
        }
        else {
            return this.tables[table_name] as type_table;
        }
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

        // Ensure the directory exists where the file will be stored
        const dirname = path.dirname(this.output_file_path);
        await fs.mkdir(dirname, { recursive: true });
        await fs.writeFile(this.output_file_path, data)

        await Promise.all([tables.map(table => table.output_to_file())]);
    }

    read_from_file = async () => {
        console.log('Reading from file');

        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);
            let { dbname, tables, storage_location } = parsed_data;

            this.dbname = dbname;
            this.storage_location = storage_location;

            // Collecting promises for each table read operation
            const tableReadPromises = tables.map((table_info: any) => {
                let { table_name, indices, primary_key } = table_info;
                // Assume add_table returns an instance with a read_from_file method
                table_info.table_obj = this.add_table({ table_name, indices, primary_key, proto: null });
                // Start reading from file and return the promise to be awaited
                return table_info.table_obj.read_from_file();
            });

            // Wait for all table read operations to complete
            await Promise.all(tableReadPromises);

            console.log('All available tables have been read from file');

        }
        catch (error) {
            console.log('Error reading from file', error, this.output_file_path)
        }


        return;
    }
}