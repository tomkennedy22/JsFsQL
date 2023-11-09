import { database } from "./index";
import fs from "fs/promises";
import path from "path";


const test = async () => {
    console.log(`\n\n\n\n\n\n\n Starting new test suite!`)

    let folder_path = path.resolve(__dirname, `../databases/`)

    let db = new database({ dbname: "test_db", folder_path });
    await db.read_from_file();

    console.log({db})
    console.log({persion: db.tables.person})


    // db.add_table({ table_name: "person", indices: ["birth_year", "birth_state"], primary_key: "person_id" });

    // let data = [
    //     { person_id: 1, name: "John", birth_year: 1994, birth_state: "CA", "hair_color": "brown" },
    //     { person_id: 2, name: "Jane", birth_year: 1993, birth_state: "NY", "hair_color": "brown" },
    //     { person_id: 3, name: "Joe", birth_year: 1992, birth_state: "TX", "hair_color": "brown" },
    //     { person_id: 4, name: "Jack", birth_year: 1991, birth_state: "CA", "hair_color": "brown" },
    //     { person_id: 5, name: "Jill", birth_year: 1990, birth_state: "NY", "hair_color": "brown" },
    //     { person_id: 6, name: "Jim", birth_year: 1994, birth_state: "TX", "hair_color": "brown" },
    //     { person_id: 7, name: "Jenny", birth_year: 1990, birth_state: "CA", "hair_color": "blonde" },
    //     { person_id: 8, name: "Jen", birth_year: 1991, birth_state: "NY", "hair_color": "brown" },
    //     { person_id: 9, name: "Jesse", birth_year: 1992, birth_state: "TX", "hair_color": "brown" },
    //     { person_id: 10, name: "Tommy", birth_year: 1992, birth_state: "CA", "hair_color": "brown" },
    //     { person_id: 11, name: "Tim", birth_year: 1993, birth_state: "NY", "hair_color": "black" },
    //     { person_id: 12, name: "Tina", birth_year: 1994, birth_state: "TX", "hair_color": "black" },
    //     { person_id: 13, name: "Terry", birth_year: 1990, birth_state: "CA", "hair_color": "black" },
    //     { person_id: 14, name: "Tara", birth_year: 1991, birth_state: "NY", "hair_color": "red" },
    //     { person_id: 15, name: "Troy", birth_year: 1992, birth_state: "TX", "hair_color": "red" },
    //     { person_id: 16, name: "Tanya", birth_year: 1993, birth_state: "CA", "hair_color": "red" },
    //     { person_id: 17, name: "Michael", birth_year: 1994, birth_state: "NY", "hair_color": "blonde" },
    //     { person_id: 18, name: "Molly", birth_year: 1990, birth_state: "TX", "hair_color": "red" },
    //     { person_id: 19, name: "Manny", birth_year: 1991, birth_state: "CA", "hair_color": "brown" },
    //     { person_id: 20, name: "Mandy", birth_year: 1992, birth_state: "NY", "hair_color": "blonde" },
    //     { person_id: 21, name: "Matt", birth_year: 1993, birth_state: "TX", "hair_color": "blonde" },
    //     { person_id: 22, name: "Marge", birth_year: 1994, birth_state: "CA", "hair_color": "blonde" },
    //     { person_id: 23, name: "Martha", birth_year: 1990, birth_state: "NY", "hair_color": "blonde" },
    //     { person_id: 24, name: "Mark", birth_year: 1991, birth_state: "TX", "hair_color": "blonde" },
    // ]

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

    // db.save_database()
}

test();