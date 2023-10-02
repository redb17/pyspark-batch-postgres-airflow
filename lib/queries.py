highest_score_per_event_query = '''
    WITH RANKED_EVENTS AS
    (SELECT EVENT_CODE,
        STUDENT_ID,
        STUDENT_NAME,
        SCORE,
        ROW_NUMBER() OVER (PARTITION BY EVENT_CODE ORDER BY SCORE DESC) AS RANK
    FROM (SELECT STUDENT_ID,
            STUDENT_NAME,
            EVENT_CODE,
            COUNT(*) AS TOTAL,
            SUM(CORRECT) / COUNT(*) * 100 AS SCORE
        FROM (SELECT attempts_df_tbl.STUDENT_ID,
                students_df_tbl.STUDENT_NAME,
                attempts_df_tbl.EVENT_CODE,
                attempts_df_tbl.SELECTED_ANSWER_ID,
                questions_df_tbl.ANSWER_OPTION_CORRECT,
                CASE
                    WHEN attempts_df_tbl.SELECTED_ANSWER_ID = questions_df_tbl.ANSWER_OPTION_CORRECT 
                        THEN 1 
                    ELSE 0
                END AS CORRECT
            FROM attempts_df_tbl
            LEFT JOIN students_df_tbl ON attempts_df_tbl.STUDENT_ID = students_df_tbl.STUDENT_ID
            LEFT JOIN questions_df_tbl ON attempts_df_tbl.QUESTION_ID = questions_df_tbl.QUESTION_ID)
        GROUP BY STUDENT_ID,
            STUDENT_NAME,
            EVENT_CODE))
    SELECT *
    FROM RANKED_EVENTS
    WHERE RANK = 1;
'''

top_10_most_popular_exams_query = '''
    SELECT attempts_df_tbl.EXAM_ID,
        exams_df_tbl.EXAM_NAME,
        COUNT(*) AS TOTAL_ATTEMPTS
    FROM attempts_df_tbl
    LEFT JOIN exams_df_tbl 
    ON attempts_df_tbl.EXAM_ID = exams_df_tbl.EXAM_ID
    GROUP BY attempts_df_tbl.EXAM_ID,
        exams_df_tbl.EXAM_NAME
    ORDER BY COUNT(*) DESC
    LIMIT 10;
'''

top_3_most_popular_exams_each_place_query = '''
    WITH RANKED_PLACES AS
    (SELECT PLACE,
        EXAM_ID,
        EXAM_NAME,
        TOTAL_ATTEMPTS,
        ROW_NUMBER() OVER (PARTITION BY PLACE ORDER BY TOTAL_ATTEMPTS DESC) AS RANK
    FROM
        (SELECT students_df_tbl.PLACE,
            attempts_df_tbl.EXAM_ID,
            exams_df_tbl.EXAM_NAME,
            COUNT(*) AS TOTAL_ATTEMPTS
        FROM attempts_df_tbl
        LEFT JOIN exams_df_tbl ON attempts_df_tbl.EXAM_ID = exams_df_tbl.EXAM_ID
        LEFT JOIN students_df_tbl ON attempts_df_tbl.STUDENT_ID = students_df_tbl.STUDENT_ID
        GROUP BY students_df_tbl.PLACE,
            attempts_df_tbl.EXAM_ID,
            exams_df_tbl.EXAM_NAME
        ORDER BY students_df_tbl.PLACE,
            attempts_df_tbl.EXAM_ID,
            exams_df_tbl.EXAM_NAME))
    SELECT *
    FROM RANKED_PLACES
    WHERE RANK <= 3;
'''

create_top_3_most_popular_exams_each_place_query = '''
    CREATE TABLE IF NOT EXISTS `top_3_most_popular_exams_each_place` (
        place VARCHAR(20) NOT NULL,
        exam_id INTEGER NOT NULL,
        exam_name STRING NOT NULL,
        total_attempts INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        attempt_dt DATE NOT NULL
    );
'''
