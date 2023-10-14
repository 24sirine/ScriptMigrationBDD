import pyodbc
import mysql.connector
 #SirineBourbiaa
# Database connection details for Microsoft SQL Server
server = 'DESKTOP-GEEVNVQ\BCDEMO'
database = 'Demo Database BC (21-0)'

# Database connection details for PHPMyAdmin - MySQL
y_db_host = 'localhost'
y_db_name = 'DynamixServices_POS'
y_db_user = 'sirine'
y_port = 4000
y_db_password = 'user1'

try:
    # Connect to the source database (Microsoft SQL Server)
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        rf'SERVER={server};'
        rf'DATABASE={database};'
        r'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Select data from the source table
    cursor.execute("SELECT [No_], [Unit Price], [Description], [Picture], [Description] FROM [CRONUS France S_A_$Item$437dbf0e-84ff-417a-965d-ed2bb9650972]")

    # Fetch all rows from the result set
    rows1 = cursor.fetchall()

  # Select data from the source table
    cursor.execute("SELECT [Code], [Name], [Address], [City], [Phone No_],[Post Code],[Country_Region Code] FROM [CRONUS France S_A_$Location$437dbf0e-84ff-417a-965d-ed2bb9650972]")

    # Fetch all rows from the result set
    rows2 = cursor.fetchall()




    # Connect to the target database (PHPMyAdmin - MySQL)
    y_conn = mysql.connector.connect(
        host=y_db_host, port=y_port, database=y_db_name, user=y_db_user, password=y_db_password)
    y_cursor = y_conn.cursor()
      


       # Delete all data from the table t_article
    delete_query1 = "DELETE FROM t_article"
    y_cursor.execute(delete_query1)
     # Delete all data from the table t_magasin
    delete_query2 = "DELETE FROM t_magasin"
    y_cursor.execute(delete_query2)
    y_conn.commit()
    print("All data deleted from the t_article table.")
    print("All data deleted from the t_magasin table.")
 
    # Iterate over the rows and insert data into the target table
    for row in rows1:
        article_code = row[0]
        prix_unitaire = row[1]
        libelle = row[2]
        url_photo = row[3]
        description = row[4]

        # Insert the data into the target table
        insert_query = "INSERT INTO T_Article (ARTICLE_CODE, ARTICLE_PRIX_UNITAIRE, ARTICLE_LIBELLE, ARTICLE_URLIMG, ARTICLE_DESCRIPTION) VALUES (%s, %s, %s, %s, %s)"
        values = (article_code, prix_unitaire, libelle, url_photo, description)
        y_cursor.execute(insert_query, values)
        

        # Extract the corresponding quantity from the source table
        quantity_query = f"SELECT ISNULL(SUM([Remaining Quantity]), 0) FROM [CRONUS France S_A_$Item Ledger Entry$437dbf0e-84ff-417a-965d-ed2bb9650972] WHERE [Item No_] = '{article_code}'"
        cursor.execute(quantity_query)
        quantity_result = cursor.fetchone()

        if quantity_result:
            article_quantity = quantity_result[0]

            # Update the corresponding quantity in the target table
            update_query = "UPDATE T_Article SET ARTICLE_QTE_STOCK = %s WHERE ARTICLE_CODE = %s"
            update_values = (article_quantity, article_code)
            y_cursor.execute(update_query, update_values)

            

            # Extract the corresponding REMISE from the source table
        remise_query = f"SELECT ISNULL(SUM([Line Discount _]), 0) FROM [CRONUS France S_A_$Purchase Line Discount$437dbf0e-84ff-417a-965d-ed2bb9650972] WHERE [Item No_] = '{article_code}' "
        cursor.execute(remise_query)
        remise_result = cursor.fetchone()
        
        if remise_result :
            article_remise = remise_result[0]
        
        

            # Update the corresponding remise in the target table
            update_query2 = "UPDATE T_Article SET ARTICLE_REMISE = %s WHERE ARTICLE_CODE = %s"
            update_values2 = (article_remise, article_code)
            y_cursor.execute(update_query2, update_values2)


 # Iterate over the rows and insert data into the target table
    for row in rows2:
        code = row[0]
        name= row[1]
        adresse = row[2]
        ville = row[3]
        tel = row[4]
        codePostal = row[5]
        codePays = row[6]
        

        # Insert the data into the target table
        insert_query2 = "INSERT INTO T_Magasin (CODE_MAGASIN,NOM_MAGASIN, ADRESSE_MAGASIN, CODEPOSTALE_MAGASIN, VILLE_MAGASIN,CODEPAYS_MAGASIN,TEL_MAGASIN) VALUES (%s, %s, %s, %s, %s,%s ,%s)"
        values2 = (code, name, adresse, codePostal, ville,codePays, tel)
        y_cursor.execute(insert_query2, values2)


    y_conn.commit()
    print('Data migrated successfully!')

except pyodbc.Error as error:
    print('Error connecting to the source database:', error)

except mysql.connector.Error as error:
    print('Error connecting to the target database:', error)

finally:
    # Close the database connections
    if 'conn' in locals():
        conn.close()
    if 'y_conn' in locals():
        y_cursor.close()
        y_conn.close()
        print('Connections closed.')








