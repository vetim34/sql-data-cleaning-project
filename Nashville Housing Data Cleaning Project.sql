--- Data Cleaning For Nashville Housing Data
CREATE TABLE Nash (
    UniqueID INT,
    ParcelID VARCHAR(120),
    LandUse VARCHAR(150),
    PropertyAddress VARCHAR(150),
    SaleDate DATE,
    SalePrice varchar(35),
    LegalReference VARCHAR(120),
    SoldAsVacant boolean,
    OwnerName VARCHAR(150),
    OwnerAddress VARCHAR(100),
    Acreage DECIMAL(10, 2),
    TaxDistrict VARCHAR(150),
    LandValue INT,
    BuildingValue INT,
    TotalValue INT,
    YearBuilt INT,
    Bedrooms INT,
    FullBath INT,
    HalfBath INT
);

copy public.nash
from 'C:\Users\USER\Desktop\10alytics\Alex Bootcamp\Nashville Housing Data for Data Cleaning.csv'
delimiter ',' csv header;

select *
from nash;

-- there are some rows with characters in the salesprice column hence the varchar datatype
-- Remove any symbols like '$' or ',' from the SalePrice column
UPDATE nash
SET SalePrice = CAST(REPLACE(REPLACE(SalePrice, '$', ''), ',', '') AS INT)
WHERE SalePrice ~ '[^0-9]' -- the ^ helps to avoid rows that contain only numeric values
-- the '~' finds similarities within the column

-- After the cleaning above, we alter the column to INT datatype
ALTER TABLE nash
ALTER COLUMN SalePrice TYPE INT USING SalePrice::INT;
---the :: operator is used for type casting, which is the conversion of one data type to another

select *
from nash;

------NOW FOR FURTHER CLEANING OF THE DATASET
SELECT PROPERTYADDRESS
FROM NASH
where propertyaddress is null;

-- 1
-- to fill in some blanks, we'll like to put fill in the property ardress where the parcelid is the same
select n.parcelid, n.propertyaddress, b.parcelid, b.propertyaddress
from nash n
join nash b
on n.parcelid = b.parcelid
and n.uniqueid <> b.uniqueid
where n.propertyaddress is null;

-- the next thing I'm doing is to use the ISNULL or COALESCE (this works with postgres) syntax to populate the null address with correspondingly similar address
select n.parcelid, n.propertyaddress, b.parcelid, b.propertyaddress, coalesce(n.propertyaddress, b.propertyaddress)
from nash n
join nash b
on n.parcelid = b.parcelid
and n.uniqueid <> b.uniqueid
where n.propertyaddress is null;
-- the coalesce function replaces the null values in table n with that in table b

-- next we update the table

UPDATE nash AS n
SET propertyaddress = b.propertyaddress
FROM nash AS b
WHERE n.parcelid = b.parcelid
AND n.uniqueid <> b.uniqueid
AND n.propertyaddress IS NULL;
-- so if you run the previous query, there wouldn't be a null value again

-- 2
--Breaking out address into individual columns
select propertyaddress
from nash

----- we'll use a SUBSTRING and CHARINDEX (it is POSITION for postgresql)
select
substring(propertyaddress from 1 for position(',' in propertyaddress) -1) as address
from nash;-- the -1 removes the comma


select *
from nash
-- using parsename instead of substring to separate the location column

select owneraddress
from nash;
-- the parsename function works with '.', so we'll replace the commas ','
select
parsename(replace(owneraddress, ',', '.'), 3) AS street,
parsename(replace(owneraddress, ',', '.'), 2) AS city,
parsename(replace(owneraddress, ',', '.'), 1) AS state
from nash -- but this doesn't work on postgres hence we'll use the split_part function below


SELECT
    SPLIT_PART(owneraddress, ',', 1) AS street,
    SPLIT_PART(owneraddress, ',', 2) AS city,
    SPLIT_PART(owneraddress, ',', 3) AS state
FROM nash;

ALTER TABLE nash -- this actually alters the table by adding the splits as new columns
add ownersplitstreet varchar(255);

UPDATE nash
SET ownersplitstreet = SPLIT_PART(owneraddress, ',', 1) 

ALTER TABLE nash
add ownersplitcity varchar(255);

UPDATE nash
SET ownersplitcity = SPLIT_PART(owneraddress, ',', 2) 

ALTER TABLE nash
add ownersplitstate varchar(255);

UPDATE nash
SET ownersplitstate = SPLIT_PART(owneraddress, ',', 3) 

------- Checking for distinct values in soldasvacant column
select distinct(soldasvacant), count(soldasvacant)
from nash
group by 1;

-- removing duplicates using CTE
with rownumcte as(
select *,
    row_number() over(
        partition by parcelid,
                     propertyaddress,
                     saleprice,
                     saledate,
                     legalreference
                     order by uniqueid
                     ) row_num
from nash
)
select *
from rownumcte
where row_num > 1

-- with this cte i can see the duplicate rows
-- to delete it, we do the below
with rownumcte as(
select *,
    row_number() over(
        partition by parcelid,
                     propertyaddress,
                     saleprice,
                     saledate,
                     legalreference
                     order by uniqueid
                     ) row_num
from nash
)
delete from nash
where (parcelid, propertyaddress, saleprice, saledate, legalreference) in (
    select parcelid, propertyaddress, saleprice, saledate, legalreference
    from rownumcte
    where row_num > 1
);

--- Delete Unused Columns
-- but don't do it for the main data

alter table nash
drop column owneraddress, 
drop column taxdistrict;

select *
from nash;
