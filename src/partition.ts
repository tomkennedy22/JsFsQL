import fs from "fs/promises";
import path from "path";
import { type_partition, type_partition_index } from "./types";
import { deep_copy, get_from_dict, partition_name_from_partition_index, set_to_dict } from "./utils";

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
            const rowPk = get_from_dict(row, this.primary_key);
            if (rowPk === undefined) {
                throw new Error(`Primary key value missing in the data row. Cannot insert into partition. Table ${this.partition_name} and primary key ${this.primary_key} and value ${rowPk}`);
            }
            else if (this.data.hasOwnProperty(rowPk)) {
                throw new Error(`Duplicate primary key value: ${rowPk} for field ${this.primary_key} in partition ${this.partition_name}`);
            }
            set_to_dict(this.data, rowPk, row);

            // Mark the dataset as 'dirty' to indicate that the state has changed   
            this.is_dirty = true;
        });

    }

    update(row: any, fields_to_drop?: any[]): void {
        const rowPk = get_from_dict(row, this.primary_key);
        if (!this.data.hasOwnProperty(rowPk)) {
            throw new Error(`Row with primary key ${rowPk} does not exist in partition ${this.partition_name}.`);
        }

        // Drop fields if necessary
        if (fields_to_drop) {
            let copied_row = deep_copy(row);
            for (const field of fields_to_drop) {
                delete copied_row[field];
            }
            set_to_dict(this.data, rowPk, copied_row);
        }
        else {
            set_to_dict(this.data, rowPk, row);
        }

        // Update the row and mark partition as dirty
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
        // // console.trace('Reading from file')
        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);

            let { partition_name, partition_indices, data: partition_data, storage_location, primary_key } = parsed_data;

            // // console.log({ partition_name, partition_indices, partition_data, storage_location, primary_key })

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
            // console.log('Error reading from file', error, this.output_file_path)
        }
    }

    delete_file = async () => {
        try {
            this.data = {};

            // Delete the file associated with this partition
            await fs.unlink(this.output_file_path);
            // console.log(`Deleted file at: ${this.output_file_path}`);
        } catch (error) {
            // Handle possible errors, such as file not existing
            console.error(`Error deleting file at: ${this.output_file_path}`, error);
        }

        return Promise.resolve();
    }
}