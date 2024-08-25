SELECT * FROM public.layoffs

--Data cleaniing 

---- remove duplicates 
---- standerdize dat 
---- null values and blank values 

	

CREATE TABLE layoffs_stgings AS
TABLE public.layoffs
WITH NO DATA;

select * from public.layoffs_stgings


INSERT INTO public.layoffs_stgings
SELECT *
FROM public.layoffs;




------------------------------------------------------------------------------- find a duplicate values 
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, "date" ORDER BY (SELECT NULL)) AS row_num
    FROM layoffs_stgings
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


------------------------------------------------------------------------------- find sepcific 
select * from public.layoffs_stgings
where company = 'Oda';

------------------------------------------------------------------------------ standardizing data 
SELECT
  company AS original_company,
  UPPER(TRIM(REPLACE(company, ' ', ''))) AS standardized_company
FROM
  public.layoffs_stgings;

update public.layoffs_stgings
set company = Trim(company)
	
---- specific selection 
select * from public.layoffs_stgings
where country like 'United States%'
order by 1;

---- anaylizing null values 
select * from public.layoffs_stgings
where total_laid_off is null
	and percentage_laid_off is null;

select distinct industry 
from public.layoffs_stgings
where total_laid_off is null;


UPDATE public.layoffs_stgings
SET industry = 'tech'
WHERE industry = 'https://www.calcalistech.com/ctechnews/article/rysmrkfua';


select distinct industry 
from public.layoffs_stgings
where industry is null 
or industry ='';


SELECT * 
FROM public.layoffs_stgings t1
JOIN public.layoffs_stgings t2
ON t1.company = t2.company
AND t1.location = t2.location  
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

----------------------------------------------------------  Exploratory Data Analysis

select * from public.layoffs_stgings

select max(total_laid_off)
from public.layoffs_stgings

	
select max(total_laid_off),max(percentage_laid_off)
from public.layoffs_stgings

	
select * from public.layoffs_stgings
where percentage_laid_off = 1
order by total_laid_off desc;



select company, sum(total_laid_off)
from public.layoffs_stgings
group by company
order by sum(total_laid_off) desc;



select min('date'),max('date')
from public.layoffs_stgings;

select industry , sum(total_laid_off)
from public.layoffs_stgings
group by industry 
order by sum(total_laid_off) desc;



select * from public.layoffs_stgings

select country , sum(total_laid_off)
from public.layoffs_stgings
group by country 
order by sum(total_laid_off) desc;




select date, sum(total_laid_off)
from public.layoffs_stgings
group by date
order by sum(total_laid_off)  desc



	

select stage, sum(total_laid_off)
from public.layoffs_stgings
group by stage
order by sum(total_laid_off)  desc;



WITH ROLLING_TOTAL AS 
(
    SELECT 
        TO_CHAR(date, 'YYYY-MM') AS "MONTH", 
        SUM(total_laid_off) AS total_laid_off_per_month
    FROM 
        public.layoffs_stgings
    WHERE 
        date IS NOT NULL 
    GROUP BY 
        "MONTH"
    ORDER BY 
        "MONTH"
)
SELECT 
    "MONTH", 
    total_laid_off_per_month
FROM 
    ROLLING_TOTAL;


WITH ROLLING_TOTAL AS 
(
    SELECT 
        TO_CHAR(date, 'YYYY-MM') AS "MONTH", 
        company, 
        SUM(total_laid_off) AS total_laid_off_per_month
    FROM 
        public.layoffs_stgings
    WHERE 
        date IS NOT NULL 
    GROUP BY 
        "MONTH", 
        company
    ORDER BY 
        "MONTH", 
        company
)
SELECT 
    "MONTH", 
    company, 
    total_laid_off_per_month
FROM 
    ROLLING_TOTAL;





WITH ROLLING_TOTAL AS 
(
    SELECT 
        company, 
        EXTRACT(YEAR FROM date) AS "YEAR",
        date,
        SUM(total_laid_off) AS total_laid_off_per_day
    FROM 
        public.layoffs_stgings
    WHERE 
        date IS NOT NULL 
    GROUP BY 
        company, 
        EXTRACT(YEAR FROM date),
        date
    ORDER BY 
        company, 
        EXTRACT(YEAR FROM date),
        date
)
SELECT 
    company, 
    "YEAR", 
    date,
    total_laid_off_per_day
FROM 
    ROLLING_TOTAL;






SELECT 
    company, 
    EXTRACT(YEAR FROM date) AS "YEAR", 
    SUM(total_laid_off) AS total_laid_off_per_year
FROM 
    public.layoffs_stgings
GROUP BY 
    company, 
    "YEAR"
ORDER BY 
    total_laid_off_per_year DESC;





WITH company_year AS 
(
    SELECT 
        company, 
        EXTRACT(YEAR FROM date) AS "YEAR", 
        SUM(total_laid_off) AS total_laid_off_per_year
    FROM 
        public.layoffs_stgings
    GROUP BY 
        company, 
        "YEAR"
)
SELECT * dense_rank () over (partition  by years order by total_laid_off desc)
FROM company_year;




WITH company_year AS 
(
    SELECT 
        company, 
        EXTRACT(YEAR FROM date) AS "YEAR", 
        SUM(total_laid_off) AS total_laid_off_per_year
    FROM 
        public.layoffs_stgings
    GROUP BY 
        company, 
        "YEAR"
), company_year_rank as 
SELECT 
    company, 
    "YEAR", 
    total_laid_off_per_year,
    DENSE_RANK() OVER (PARTITION BY "YEAR" ORDER BY total_laid_off_per_year DESC) AS rank
FROM 
    company_year
)
select * from company _year_rank 
	where ranking<=5
;







WITH company_year AS 
(
    SELECT 
        company, 
        EXTRACT(YEAR FROM date) AS "YEAR", 
        SUM(total_laid_off) AS total_laid_off_per_year
    FROM 
        public.layoffs_stgings
    GROUP BY 
        company, 
        "YEAR"
), company_year_rank AS 
(
    SELECT 
        company, 
        "YEAR", 
        total_laid_off_per_year,
        DENSE_RANK() OVER (PARTITION BY "YEAR" ORDER BY total_laid_off_per_year DESC) AS rank
    FROM 
        company_year
)
SELECT * 
FROM 
    company_year_rank 
WHERE 
    rank <= 5;





