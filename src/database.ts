import fs from "fs/promises";
import path from "path";
import { type_database, type_table } from "./types";
import { table } from "./table";


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

    add_table({ table_name, indices, primary_key, proto }: { table_name: string, indices: string[], primary_key: string, proto?: any }): type_table {

        if (!table_name) {
            throw new Error('Table name is required');
        }

        if (!this.tables.hasOwnProperty(table_name)) {
            let new_table = new table({ table_name, indices, storage_location: this.storage_location, dbname: this.dbname, primary_key, proto });
            this.tables[table_name] = new_table;
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
        // console.log('Reading from file');

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

            // console.log('All available tables have been read from file');

        }
        catch (error) {
            // console.log('Error reading from file', error, this.output_file_path)
        }


        return;
    }
}