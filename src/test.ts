import { database } from "./index";
import fs from "fs/promises";
import path from "path";


const test = async () => {
    console.log(`\n\n\n\n\n\n\n Starting new test suite!`)

    let folder_path = path.resolve(__dirname, `../databases/`)

    let db = new database({ dbname: "test_db", folder_path });
    await db.read_from_file();

    console.log({ db })
    console.log({ persion: db.tables.person })


    // db.add_table({ table_name: "person", indices: ["birth_year", "birth_state"], primary_key: "person_id" });

    // let data = []

    // db.tables.person.insert(data);

    // let results_1994_CA = db.tables.person.find({birth_year: 1994, birth_state: "CA"});
    // let results_1994 = db.tables.person.find({birth_year: 1994});
    // let results_CA = db.tables.person.find({birth_state: "CA"});
    // let results_pid_1 = db.tables.person.findOne({person_id: 1});
    let results_pid_1_eq = db.tables.person.findOne({ person_id: { $eq: 1 } });
    // let results_matts = db.tables.person.find({name: "Matt"});
    // let results_matt = db.tables.person.findOne({name: "Matt"});

    // console.log({results_1994_CA, results_1994, results_CA, results_matts, results_matt, results_pid_1, results_pid_1_eq})

    // let results_1990_1994 = db.tables.person.find({$or: [{birth_year: 1990}, {birth_year: 1994}]});
    // let results_gt_1992 = db.tables.person.find({birth_year: {$gt: 1992}});
    // let results_gte_1993 = db.tables.person.find({birth_year: {$gte: 1993}});

    // console.log({results_1990_1994, results_gt_1992, results_gte_1993, results_pid_1, results_pid_1_eq})
    console.log({ results_pid_1_eq })

    await db.save_database()
}

test();