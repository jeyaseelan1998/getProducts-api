const express = require('express');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const path = require('path');
const fetch = require('node-fetch')

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

const getStatisticsOfGivenMonth = async (month) => {
    const soldProductsCountQuery = `
        SELECT 
        SUM(
            CASE 
            WHEN sold = true THEN price
            ELSE 0
            END
        ) as total_sale_amount,
        COUNT(
            CASE 
            WHEN sold = true THEN 1
            END
        ) as sold_products_count,
        COUNT(
            CASE 
            WHEN sold = false THEN 1
            END
        ) as unsold_products_count
        FROM product
        WHERE CAST(strftime('%m', date_of_sale) AS INT) = ${month};
    `;
    const dbResponse = await db.get(soldProductsCountQuery)
    return dbResponse
}

app.get("/statistics/:monthId", async (request, response) => {
    const {monthId} = request.params
    const dbResponse = await getStatisticsOfGivenMonth(monthId)
    response.send(dbResponse)
})

const getBarchartDataForGivenMonth = async (month) => {
    const soldProductsCountQuery = `
        SELECT 
        COUNT(
            CASE 
            WHEN price < 101 THEN 1
            END
        ) as '0-100',
        COUNT(
            CASE 
            WHEN price > 100 and price < 201 THEN 1
            END
        ) as '100-200',
        COUNT(
            CASE 
            WHEN price > 200 and price < 301 THEN 1
            END
        ) as '200-300',
        COUNT(
            CASE 
            WHEN price > 300 and price < 401 THEN 1
            END
        ) as '300-400',
        COUNT(
            CASE 
            WHEN price > 400 and price < 501 THEN 1
            END
        ) as '400-500',
        COUNT(
            CASE 
            WHEN price > 500 and price < 601 THEN 1
            END
        ) as '500-600',
        COUNT(
            CASE 
            WHEN price > 600 and price < 701 THEN 1
            END
        ) as '600-700',
        COUNT(
            CASE 
            WHEN price > 700 and price < 801 THEN 1
            END
        ) as '700-800',
        COUNT(
            CASE 
            WHEN price > 800 and price < 901 THEN 1
            END
        ) as '800-900',
        COUNT(
            CASE 
            WHEN price > 900 THEN 1
            END
        ) as 'above_900'
        FROM product
        WHERE CAST(strftime('%m', date_of_sale) AS INT) = ${month};
    `;
    const dbResponse = await db.get(soldProductsCountQuery)
    return dbResponse
}

app.get("/bar-chart/:monthId", async (request, response) => {
    const {monthId} = request.params
    const dbResponse = await getBarchartDataForGivenMonth(monthId)
    response.send(dbResponse)
})

const getPieChartOfGivenMonth = async (month) => {
    const getPieChartQuery = `
        SELECT 
        category, count() as no_of_item
        FROM product
        WHERE CAST(strftime('%m', date_of_sale) AS INT) = ${month}
        GROUP BY category;
    `;
    const dbResponse = await db.all(getPieChartQuery)
    return dbResponse
}

app.get("/pie-chart/:monthId", async (request, response) => {
    const {monthId} = request.params
    const dbResponse = await getPieChartOfGivenMonth(monthId)
    response.send(dbResponse)
})


app.get("/all-data/:monthId", async (request, response) => {
    const {monthId} = request.params
    const statisticsData = await getStatisticsOfGivenMonth(monthId)
    const barChartData = await getBarchartDataForGivenMonth(monthId)
    const pieCartData = await getPieChartOfGivenMonth(monthId)

    const formatedData = {statisticsData, barChartData, pieCartData}

    response.send(formatedData)

})