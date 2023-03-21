
 select * from c where array_contains(c.tags,"database")
-- 2.
select * from c where c.author.profile.courses = 2

-- 3.
select * from c where c.tags = ["language","freeware","programming"]

--4.
select * from c where array_contains(c.tags,"programming")

-- 5.
select * from c where array_contains(c.languages,"telugu")

-- 6.
select * from c

-- 7.
select count("id") from c

-- 8.
select top 1 * from c


-- 9.
select * from c where c.no_of_reviews >3 or array_contains(c.tags,"programming")


--10 
select * from c where c.no_of_reviews < 3 or c.downloadable = true or c.author.profile.books >= 2

-- 11
select * from c where c.no_of_reviews > 3

-- 12 
select * from c where c.tags = ["programming"]

-- 13
select * from c where c.tags = ["language","freeware","programming"]

-- 14
select * from c where c.no_of_reviews != 3

-- 15 
select * from c where array_contains(c.tags,"database") or array_contains(c.tags,"programming")