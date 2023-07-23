const express = require('express');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const path = require('path');

const app = express();
let db = null;

const dbPath = path.join(__dirname, './products.db')

const initializeAndCreateDB = async () => {
    try {
        db = await open({
            filename: dbPath,
            driver: sqlite3.Database
        })
        app.listen(3000, () => console.log('Server started at http://localhost:3000'))
    } catch (error) {
        console.log(error);
        process.exit(1);
    }
}

initializeAndCreateDB()

const createProductTable = async data => {
    try {
        const createTableQuery = `
        CREATE TABLE product(
            id NOT NULL PRIMARY KEY,
            title TEXT,
            price FLOAT,
            description TEXT,
            category TEXT,
            image TEXT,
            sold  BOOLEAN,
            date_of_sale  DATETIME
        );`;
        await db.run(createTableQuery)

        const insertValuesQuery = `INSERT INTO product (
            id,
            title,
            price,
            description,
            category,
            image,
            sold,
            date_of_sale
        ) VALUES ` + data.map(product =>
            `(${product.id}, "${product.title}", ${product.price}, "${product.description}", "${product.category}", "${product.image}", ${product.sold}, "${product.dateOfSale}")`
        ).join(', ') + " ;"

        await db.run(insertValuesQuery)
    } catch (error) {
        console.log('Product table already exists')
    }
}

//create products table
app.get("/", async (request, response) => {
    const apiUrl = "https://s3.amazonaws.com/roxiler.com/product_transaction.json";
    const options = { method: "GET" };
    const apiResponse = await fetch(apiUrl, options)
    const data = await apiResponse.json()

    await createProductTable(data)

    const SQLQUERY = `SELECT * from product;`
    let dbResponse = await db.all(SQLQUERY)
    response.send(dbResponse)
})
