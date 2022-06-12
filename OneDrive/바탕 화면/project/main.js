const express = require('express');
const app = express();
const fs = require('fs');
const ejs = require('ejs');
const mysql = require('mysql');
const path = require('path');
const cors = require('cors');

const client = mysql.createConnection({
    host: "admin8888.cedkd2swhyq1.ap-northeast-2.rds.amazonaws.com",
    user: "admin9999",
    password: "00000000",
    database: "aws"
});
const mainPage = fs.readFileSync('./index.ejs', 'utf8');

const jsondata = fs.readFileSync('Books.json');
const books = JSON.parse(jsondata.toString());

const filelist = fs.readdirSync('image', 'utf-8');

app.use(cors());

app.get('/', (req, res) => {


    var page = ejs.render(mainPage, {
        title: "Temporary Title",
    });
    res.send(page);

});

app.get('/book', (_req, res) => {
    const ran = Math.floor(Math.random() * books.length);
    const book = books[ran];
    res.json(book);
});
app.get('/image/:bookno', (req, res) => {
    const idx = books.findIndex(book => book.No.toString() === req.params.bookno);
    const filename = filelist[idx];
    const filepath = path.resolve(path.join(__dirname, `image/${filename}`));
    res.sendFile(filepath);
});

app.get('/getdata', (req, res) => {


	client.query("SELECT * FROM (SELECT lowage, gender, JSON_EXTRACT(emotions, '$[0].Confidence')AS Confidence0 , JSON_EXTRACT(emotions, '$[1].Confidence')AS Confidence1 , JSON_EXTRACT(emotions, '$[2].Confidence')AS Confidence2 , JSON_EXTRACT(emotions, '$[3].Confidence')AS Confidence3 , JSON_EXTRACT(emotions, '$[0].Type')AS Type0 ,JSON_EXTRACT(emotions, '$[1].Type')AS Type1 , JSON_EXTRACT(emotions, '$[2].Type')AS Type2 , JSON_EXTRACT(emotions, '$[3].Type')AS Type3 FROM emotion ORDER BY id DESC LIMIT 1) sub ORDER BY 'id' ASC", function(err, result, fields){
        if(err) throw err;
        res.json(result);
    });
});

app.listen(3400, () => {
    console.log('Server Running on Port 3400!');
});

app.use(express.static(path.join(__dirname, 'public')));