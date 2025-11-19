import logging


logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row, table="yt_api"):
    try:
        if schema == "staging":
            # row = dict venant de l'API (snake_case)
            sql = f"""
                INSERT INTO {schema}.{table}(
                    "Video_ID",
                    "Video_Title",
                    "Upload_Date",
                    "Duration",
                    "Video_views",
                    "Likes_Count",
                    "Comments_Count"
                )
                VALUES (
                    %(video_id)s,
                    %(title)s,
                    %(publishedAt)s,
                    %(duration)s,
                    %(viewCount)s,
                    %(likeCount)s,
                    %(commentCount)s
                );
            """
            video_id_value = row["video_id"]

        else:
            # row = dict venant de la table staging (camel/pascal case)
            sql = f"""
                INSERT INTO {schema}.{table}(
                    "Video_ID",
                    "Video_Title",
                    "Upload_Date",
                    "Duration",
                    "Video_views",
                    "Likes_Count",
                    "Comments_Count"
                )
                VALUES (
                    %(Video_ID)s,
                    %(Video_Title)s,
                    %(Upload_Date)s,
                    %(Duration)s,
                    %(Video_views)s,
                    %(Likes_Count)s,
                    %(Comments_Count)s
                );
            """
            video_id_value = row["Video_ID"]

        cur.execute(sql, row)
        conn.commit()

        logger.info(f"Inserted row with VIDEO_Id: {video_id_value}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting row with VIDEO_Id: {video_id_value} - {e}")
        raise e

    
def update_rows(cur, conn, schema, row, table="yt_api"):
    """
    Met à jour une vidéo dans {schema}.{table}.

    - Si schema == 'staging' : `row` vient de l'API YouTube
      (keys: video_id, title, publishedAt, duration, viewCount, likeCount, commentCount)

    - Sinon : `row` vient de la base
      (keys: Video_ID, Video_Title, Upload_Date, Duration, Video_views, Likes_Count, Comments_Count)
    """

    try:
        # 1) Normaliser les paramètres avec les NOMS DE COLONNES de la table
        if schema == "staging":
            # row = dict issue de l’API (snake_case)
            params = {
                "Video_ID":       row["video_id"],
                "Video_Title":    row["title"],
                "Upload_Date":    row["publishedAt"],
                "Duration":       row["duration"],        # ou une version transformée
                "Video_views":    row["viewCount"],
                "Likes_Count":    row["likeCount"],
                "Comments_Count": row["commentCount"],
            }
        else:
            # row = dict venant déjà de la table (SELECT * FROM core.yt_api)
            params = {
                "Video_ID":       row["Video_ID"],
                "Video_Title":    row["Video_Title"],
                "Upload_Date":    row["Upload_Date"],
                "Duration":       row["Duration"],
                "Video_views":    row["Video_views"],
                "Likes_Count":    row["Likes_Count"],
                "Comments_Count": row["Comments_Count"],
            }

        # 2) SQL avec uniquement les noms de colonnes (pas les noms de variables Python)
        sql = f"""
        UPDATE {schema}.{table}
        SET
            "Video_Title"    = %(Video_Title)s,
            "Duration"       = %(Duration)s,
            "Video_views"    = %(Video_views)s,
            "Likes_Count"    = %(Likes_Count)s,
            "Comments_Count" = %(Comments_Count)s
        WHERE
            "Video_ID"   = %(Video_ID)s
        AND "Upload_Date" = %(Upload_Date)s;
        """

        # 3) Exécution
        cur.execute(sql, params)
        conn.commit()

        logger.info(
            f"Updated row in {schema}.{table} with VIDEO_Id={params['Video_ID']}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(
            f"Error updating row in {schema}.{table} with VIDEO_Id={params.get('Video_ID')} - {e}"
        )
        raise e




def delete_rows(cur, conn, schema, id_to_delete):

    try:

        id_to_delete = f"""({', '.join(f"'{id}'" for id in id_to_delete)})"""
        
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {id_to_delete};
            """
        )

        conn.commit()

        logger.info(f"Deleted row with VIDEO_Id: {id_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting row with VIDEO_Id: {id_to_delete} - {e}")
        raise e


