--
-- variable used here are in specific order.
-- 0 and 1 represents the order in which those
-- variables are presented at run time
-- inputs : 
--  {0} - Customers dataframe
--  {1} - Orders dataframe
--  {2} - Filter condition for id 1
--  {3} - Filter condition for id 2

select  
a.id ,
a.name,
b.balance 
from 
{0} a 
join 
{1} b 
on a.id = b.id 
where
(  a.id = {2} or 
   a.id = {3}  )