/*
Creating the table and loading the dataset
*/
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data1.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN timestamp;

-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/
-- Query 1
SELECT * FROM RATINGS WHERE rating < 1.0;
-- Query 2
SELECT * FROM RATINGS WHERE rating >= 1.0 AND rating < 2.0;
-- Query 3
SELECT * FROM RATINGS WHERE rating >= 2.0 AND rating < 3.0;
-- Query 4
SELECT * FROM RATINGS WHERE rating >= 3.0 AND rating < 4.0;
-- Query 5
SELECT * FROM RATINGS WHERE rating >= 4.0;


-- Part B
/* Create the fragmentations for Part B1 */
DROP TABLE IF EXISTS B1_1;
DROP TABLE IF EXISTS B1_2;
DROP TABLE IF EXISTS B1_3;
DROP TABLE IF EXISTS B1_U;

-- Fragment 1
SELECT * INTO B1_1 FROM RATINGS WHERE rating < 3.5;
-- Fragment 2
SELECT * INTO B1_2 FROM RATINGS WHERE rating > 2.5 AND rating < 4.5;
-- Fragment 3
SELECT * INTO B1_3 FROM RATINGS WHERE rating > 3.5;

/* Write reconstruction query/queries for Part B1 */
SELECT * INTO B1_U FROM B1_1 UNION SELECT * FROM B1_2 UNION SELECT * FROM B1_3;

/* Write your explanation as a comment */
/*
Fragment Queries:
Fragment 1: Contains rows with ratings in the range of [0,3.0]
Fragment 2: Contains rows with ratings in the range of [3.0,4.0]
Fragment 3: Contains rows with ratings in the range of [4.0,5.0]

Completeness - All rows in the RATINGS table contain rating in the range of [0,5]. As the fragments are created on the criteria of
               rating span over the entire range (fragment created in the range of [0,5]), complete data is captured by the fragments.
Reconstruction - On performing UNION on all fragments B1_1, B1_2, B1_3, we are able to reconstruct the original table.
Disjointness (not satisfied) - On performing UNION ALL on all fragments, we will notice that a lot of rows/tuples are repeated.
                             - SELECT * FROM B1_1 UNION ALL SELECT * FROM B1_2 UNION ALL SELECT * FROM B1_3;
*/


/* Create the fragmentations for Part B2 */
DROP TABLE IF EXISTS B2_1;
DROP TABLE IF EXISTS B2_2;
DROP TABLE IF EXISTS B2_3;

SELECT userid INTO B2_1 FROM RATINGS;
SELECT movieid INTO B2_2 FROM RATINGS;
SELECT rating INTO B2_3 FROM RATINGS;


/* Write your explanation as a comment */
/*
Fragment Queries:
Fragment 1: Contains userid column from RATINGS table.
Fragment 2: Contains movieid column from RATINGS table.
Fragment 3: Contains rating column from RATINGS table.

Completeness - RATINGS table contains 3 columns: userid, movieid and rating. These are encapsulated in 3 fragments: B2_1, B2_2 and B2_3 respectively.
               If these fragments were to be joined, we will get the original RATINGS table.
Reconstruction (not satisfied) - As there is not foreign key amongst the fragments, there is no possible manner to construct the original table.
Disjointness - If the 3 fragments were to be combined, we will obtain the original RATINGS table without any repetition as no columns have been repeated. 
*/


/* Create the fragmentations for Part B3 */
DROP TABLE IF EXISTS B3_1;
DROP TABLE IF EXISTS B3_2;
DROP TABLE IF EXISTS B3_3;
DROP TABLE IF EXISTS B3_U;

-- Fragment 1
SELECT * INTO B3_1 FROM RATINGS WHERE rating <= 2.5;
-- Fragment 2
SELECT * INTO B3_2 FROM RATINGS WHERE rating >= 3.0 AND rating <= 4.0;
-- Fragment 3
SELECT * INTO B3_3 FROM RATINGS WHERE rating > 4.0;


/* Write reconstruction query/queries for Part B3 */
SELECT * INTO B3_U FROM B3_1 UNION SELECT * FROM B3_2 UNION SELECT * FROM B3_3;


/* Write your explanation as a comment */
/*
Fragment Queries:
Fragment 1: Contains rows with ratings in the range of [0,2.5]
Fragment 2: Contains rows with ratings in the range of [3.0,4.0]
Fragment 3: Contains rows with ratings in the range of [4.5,5.0]

Completeness - All rows in the RATINGS table contain rating in the range of [0,5]. As the fragments are created on the criteria of
               rating span over the entire range, complete data is captured by the fragments.
Reconstruction - On performing UNION on all fragments B3_1, B3_2, B3_3, we are able to reconstruct the original table.
Disjointness - On performing UNION ALL on all fragments, we notice that non of rows/tuples are repeated.
             - SELECT * FROM B3_1 UNION ALL SELECT * FROM B3_2 UNION ALL SELECT * FROM B3_3;
*/


-- Part C
/* Write the queries for Part C */
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS f3;


-- Fragment 1
SELECT * INTO f1 FROM RATINGS WHERE rating <= 2.5;
-- Fragment 2
SELECT * INTO f2 FROM RATINGS WHERE rating >= 3.0 AND rating <= 4.0;
-- Fragment 3
SELECT * INTO f3 FROM RATINGS WHERE rating > 4.0;

-- Query 1
SELECT * FROM f1 WHERE rating < 1.0;
-- Query 2
SELECT * FROM f1 WHERE rating >= 1.0 AND rating < 2.0;
-- Query 3
SELECT * FROM f1 WHERE rating >= 2.0 AND rating < 3.0;
-- Query 4
SELECT * FROM f1 WHERE rating >= 3.0 AND rating < 4.0;
-- Query 5
SELECT * FROM f1 WHERE rating >= 4.0;

-- Query 6
SELECT * FROM f2 WHERE rating < 1.0;
-- Query 7
SELECT * FROM f2 WHERE rating >= 1.0 AND rating < 2.0;
-- Query 8
SELECT * FROM f2 WHERE rating >= 2.0 AND rating < 3.0;
-- Query 9
SELECT * FROM f2 WHERE rating >= 3.0 AND rating < 4.0;
-- Query 10
SELECT * FROM f2 WHERE rating >= 4.0;

-- Query 11
SELECT * FROM f3 WHERE rating < 1.0;
-- Query 12
SELECT * FROM f3 WHERE rating >= 1.0 AND rating < 2.0;
-- Query 13
SELECT * FROM f3 WHERE rating >= 2.0 AND rating < 3.0;
-- Query 14
SELECT * FROM f3 WHERE rating >= 3.0 AND rating < 4.0;
-- Query 15
SELECT * FROM f3 WHERE rating >= 4.0;