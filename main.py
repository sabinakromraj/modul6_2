import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
    specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    """ Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_pets(conn, pets):
    """
    Create a new pet into the pets table
    :param conn:
    :param pets:
    :return: pets id
    """
    sql = '''INSERT INTO pets(name, species, age)
             VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, pets)
    conn.commit()
    return cur.lastrowid


def add_meals(conn, meals):
    """
    Create a new meal into the meals table
    :param conn:
    :param meals:
    :return: meals id
    """
    sql = '''INSERT INTO meals(pets_id, date, type_of_meal, pet_food_name, amount)
             VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, meals)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update date, type_of_meal, pet_food_name, amount of a meal
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")


if __name__ == "__main__":

    create_pets_sql = """
    -- pets table
    CREATE TABLE IF NOT EXISTS pets (
        pets_id integer PRIMARY KEY,
        name text NOT NULL,
        species text NOT NULL,
        age VARCHAR(2) NOT NULL
    );
    """

    create_meals_sql = """
    -- meals table
    CREATE TABLE IF NOT EXISTS meals (
        id integer PRIMARY KEY,
        pets_id integer NOT NULL,
        date text NOT NULL,
        type_of_meal VARCHAR(15) NOT NULL,
        pet_food_name text NOT NULL,
        amount VARCHAR(10),
        FOREIGN KEY (pets_id) REFERENCES pets (pets_id)
    );
    """

    db_file = "database.db"

    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_pets_sql)
        execute_sql(conn, create_meals_sql)

        pets_data = [
            ("Czaruś", "cat", "8"),
            ("Behemocik", "cat", "11"),
            ("Celinka", "cat", "11"),
            ("Lucynka", "dog", "2")
        ]

        meals_data = [
            ("2023-09-12 20:00:00", "supper",
             "Wiejska Zagroda Kurczak z Kaczką", "45g"),
            ("2023-09-12 20:00:00", "supper",
             "Wiejska Zagroda Kurczak z Kaczką", "55g"),
            ("2023-09-12 20:00:00", "supper",
             "Wiejska Zagroda Kurczak z Kaczką", "50g"),
            ("2023-09-12 20:00:00", "supper", "Fitmin Medium Light", "170g")
        ]

        for pet_info, meal_info in zip(pets_data, meals_data):
            p_id = add_pets(conn, pet_info)
            meal_info = (p_id,) + meal_info
            m_id = add_meals(conn, meal_info)
            print(p_id, m_id)

        print(select_all(conn, "meals"))
        print(select_where(conn, "pets", age="11"))
        update(conn, "meals", 1, amount="55g")
        print(select_where(conn, "meals", id="1"))
        delete_where(conn, "meals", pet_food_name="Fitmin Medium Light")
        print(select_all(conn, "meals"))
        delete_all(conn, "meals")
        print(select_all(conn, "meals"))

        conn.close()
