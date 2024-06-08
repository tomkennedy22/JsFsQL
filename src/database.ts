import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_database, type_join_type, type_table } from "./types";
import { table } from "./table";


export class database implements type_database {
    dbname: string;
    tables: { [key: string]: type_table<any> };
    folder_path: string;
    storage_location: string;
    output_file_path: string;
    do_compression: boolean;

    constructor({ dbname, folder_path, do_compression }: { dbname: string, folder_path: string, do_compression: boolean }) {
        this.dbname = dbname;
        this.folder_path = folder_path;
        this.tables = {};

        this.storage_location = `${folder_path}/${dbname}`;
        this.output_file_path = `${this.storage_location}/_${dbname}.json`;
        this.do_compression = do_compression;
    }

    add_table({ table_name, indices, primary_key, proto, delete_key_list }: { table_name: string, indices: string[], primary_key: string, proto?: any, delete_key_list: string[] }): type_table<any> {

        if (!table_name) {
            throw new Error('Table name is required');
        }

        if (!this.tables.hasOwnProperty(table_name)) {
            let new_table = new table({ table_name, indices, storage_location: this.storage_location, dbname: this.dbname, primary_key, proto, delete_key_list, do_compression: this.do_compression });
            this.tables[table_name] = new_table;
            return new_table as type_table<any>;
        }
        else {
            return this.tables[table_name] as type_table<any>;
        }
    }

    add_connection({ table_a_name, table_b_name, join_key, join_type }: type_connection_init): void {
        let table_a = this.tables[table_a_name];
        let table_b = this.tables[table_b_name];

        let opposite_join_type: { [key in type_join_type]: type_join_type } = {
            'one_to_many': 'many_to_one',
            'many_to_one': 'one_to_many',
            'one_to_one': 'one_to_one',
        }

        if (!table_a) {
            throw new Error(`Table does not exist for connection - ${table_a_name}`);
        }
        else if (!table_b) {
            throw new Error(`Table does not exist for connection - ${table_b_name}`);
        }


        table_a.table_connections[table_b_name] = { join_key, join_type };
        table_b.table_connections[table_a_name] = { join_key, join_type: opposite_join_type[join_type] };
    }

    save_database = async () => {
        let tables = Object.values(this.tables);
        let table_info = tables.map(table => ({ table_name: table.table_name, indices: table.indices, primary_key: table.primary_key }));

        let save_data = {
            dbname: this.dbname,
            tables: table_info,
            storage_location: this.storage_location,
            output_file_path: this.output_file_path,
            do_compression: this.do_compression,
        }

        let data = JSON.stringify(save_data, null, 2);

        const dirname = path.dirname(this.output_file_path);
        await fs.mkdir(dirname, { recursive: true });
        await fs.writeFile(this.output_file_path, data)

        await Promise.all([tables.map(table => table.output_to_file())]);
    }

    read_from_file = async () => {
        try {
            let data = await fs.readFile(this.output_file_path, 'utf-8');
            let parsed_data = JSON.parse(data);
            let { dbname, tables, storage_location, do_compression } = parsed_data;

            this.dbname = dbname;
            this.storage_location = storage_location;
            this.do_compression = do_compression;

            const tableReadPromises = tables.map((table_info: any) => {
                let { table_name, indices, primary_key, delete_key_list } = table_info;
                table_info.table_obj = this.add_table({ table_name, indices, primary_key, proto: null, delete_key_list });
                return table_info.table_obj.read_from_file();
            });

            await Promise.all(tableReadPromises);

        }
        catch (error) {
            console.log('Error reading from file', this.output_file_path, error)
        }

        return;
    }
}