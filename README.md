# clickorm-ch

**Lightweight ClickHouse ORM & query toolkit** built on top of [`clickhouse-connect`].  
Tolerant identifiers (safe quoting incl. Unicode), simple models, convenient DDL/insert/query helpers.

[PyPI](https://pypi.org/project/clickorm-ch/) • [Source](https://github.com/gagikabovyan98/clickorm-ch)

---

## 🚀 Installation

```bash
pip install clickorm-ch


⚙️ Connect
    from clickorm_ch import ClickHouseORM

    db = ClickHouseORM(
        host="127.0.0.1",
        port=8123,          # HTTP порт ClickHouse
        user="default",
        password="",
        database="default",
        secure=False,       # True → HTTPS (обычно порт 443/8443)
        timeout=30,
    )

🧱 Defining Models
    from clickorm_ch import Base, Column
    from clickorm_ch import Int64, String, Float64, Date

    class Sales(Base):
        __table__ = "sales"
        __engine__ = "MergeTree"
        __order_by__ = ["id"]

        id     = Column(Int64(), primary_key=True)
        name   = Column(String())
        amount = Column(Float64())
        date   = Column(Date())

🏗 Creating and Dropping Tables
    Sales.create(db)           # CREATE TABLE IF NOT EXISTS "sales"
    Sales.drop(db)             # DROP TABLE IF EXISTS "sales"


    Or manually:

    from clickorm_ch import create_table_from_model
    create_table_from_model(db, Sales)

💾 Inserting Data
    session = db.session()

    # Insert simple rows
    session.insert_rows(Sales, [
        [1, "Book", 12.5, "2024-01-01"],
        [2, "Pen",  2.3,  "2024-01-02"]
    ], columns=["id", "name", "amount", "date"])

    # Insert from a SELECT query
    session.insert_from_select(
        Sales,
        "SELECT number, concat('User', toString(number)), number * 2, today() FROM numbers(2)",
        columns=["id", "name", "amount", "date"]
    )


📊 Querying Data
    Model = db.generate_model("sales")   # auto-generate model from table
    session = db.session()

    rows = session.query(Model).limit(5).all()
    item = session.query(Model).filter(Model.id == 2).first()

    filtered = (
        session.query(Model)
        .filter(Model.amount > 10)
        .order_by((Model.date, "DESC"))
        .limit(3)
        .all()
    )


    🧩 Raw SQL Queries
    db.execute("SELECT count() FROM sales")
    db.scalar("SELECT max(amount) FROM sales")


📤 Streaming CSV Inserts (Async)
    import anyio

    async def upload_csv():
        async def gen():
            yield b"id,name,amount,date\n"
            yield b"1,Item1,9.9,2025-01-01\n"
            yield b"2,Item2,5.5,2025-01-02\n"

        await db.stream_csv("sales", gen(), with_names=True)

    anyio.run(upload_csv)


⚙️ Manual Table Creation
    from clickorm_ch import create_table, Int64, String, Float64

    create_table(
        db,
        name="manual_sales",
        columns={
            "id": Int64(),
            "title": String(),
            "price": Float64(),
        },
        engine="MergeTree",
        order_by=["id"]
    )

🧠 Auto-Generating Models from Tables
    XMLDataset = db.generate_model("xml_dataset")
    rows = db.session().query(XMLDataset).limit(3).all()


🧰 Supported Types

    Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Decimal, String, FixedString, UUID, Bool, Date, Date32, DateTime, DateTime64, Nullable, Array, LowCardinality

✅ Supported Features
    Feature	Status
    SELECT with filters/order/limit	✅
    INSERT (rows / SELECT)	✅
    CREATE TABLE (from model / manual)	✅
    DROP TABLE	✅
    Auto-model generation (DESCRIBE TABLE)	✅
    CSV streaming insert (async)	✅
    Nullable / Array / LowCardinality types	✅
    HTTPS connection	✅
    UPDATE / DELETE	⚠ manual via db.execute()


🔁 Full Example
    from clickorm_ch import Base, Column, Int64, String, ClickHouseORM

    db = ClickHouseORM(host="127.0.0.1", port=8123)

    class Test(Base):
        id = Column(Int64(), primary_key=True)
        note = Column(String())

    Test.create(db)

    session = db.session()
    session.insert_rows(Test, [[1, "hello"], [2, "world"]], columns=["id", "note"])

    rows = session.query(Test).limit(5).all()
    print(rows)

🧩 Development

    Built for Python 3.9+

    Based on clickhouse-connect

    All identifiers and Unicode symbols are safely quoted

    Fully HTTP-based (port 8123, optional HTTPS)