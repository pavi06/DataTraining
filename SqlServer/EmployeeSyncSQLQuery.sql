create table employee_src(
EmpId int primary key Identity,
EmpName varchar(50),
Salary decimal(10,2),
UpdateTimeFlag dateTime2 default sysdatetime()
);

create table employee_trgt(
EmpId int not null,
EmpName varchar(50),
Salary decimal(10,2),
UpdateTimeFlag dateTime2,
isActive bit
);
go


select * from employee_src
select * from employee_trgt
go

create trigger trg_UpdateEmployeeSrc
on employee_src
after update
as
begin
    update src
    set UpdateTimeFlag = GetDate()
    from employee_src src
    inner join inserted i on src.EmpId = i.EmpId;
end;
go

/*
create procedure InsertRecord
as
begin
    insert into employee_trgt (EmpId, EmpName, Salary, UpdateTimeFlag, isActive)
    select src.EmpId, src.EmpName, src.Salary, src.UpdateTimeFlag, 1
    from employee_src src
    left join employee_trgt trgt on src.EmpId = trgt.EmpId
    where trgt.EmpId is null;
end;
go
*/


/*
create or alter procedure UpdateRecord
as
begin
	with RankedEmployeeData as (
	select EmpId, EmpName, Salary, UpdateTimeFlag, isActive,
    Rank() Over (Partition By EmpId Order By UpdateTimeFlag desc) as ru
	From employee_trgt
	)

	insert into employee_trgt (EmpId, EmpName, Salary, UpdateTimeFlag, isActive)
    select src.EmpId, src.EmpName, src.Salary, src.UpdateTimeFlag, 1
    from employee_src src
	left join RankedEmployeeData rd on src.EmpId = rd.EmpId and rd.ru=1 
    where src.UpdateTimeFlag <> rd.UpdateTimeFlag;
end;
go

create procedure SoftDelete
as
begin
    update trgt
    set isActive = 0
    from employee_trgt trgt
	left join employee_src src on trgt.EmpId = src.EmpId
    where src.EmpId is null and src.isActive = 1;
end;
go
*/

/*
create or alter procedure SyncTable(@defaultFlag int=0)
as
begin
	 if @defaultFlag=1
	 begin
		with RankedEmployeeData as (
		select EmpId, EmpName, Salary, UpdateTimeFlag, isActive,
		Rank() Over (Partition By EmpId Order By UpdateTimeFlag desc) as ru
		From employee_trgt
		)select EmpId,EmpName,Salary,UpdateTimeFlag,isActive 
		from RankedEmployeeData where ru = 1;
	 end;
	 exec InsertRecord
	 exec UpdateRecord
	 exec SoftDelete
end;
go*/

create or alter procedure SyncTable(@defaultFlag int = 0)
as
begin
    if @defaultFlag = 1
    begin
        with RankedEmployeeData as (
            select EmpId, EmpName, Salary, UpdateTimeFlag, isActive,
            Rank() Over (Partition By EmpId Order By UpdateTimeFlag desc) as ru
            From employee_trgt
        )
        select EmpId, EmpName, Salary, UpdateTimeFlag, isActive
        from RankedEmployeeData
        where ru = 1;
    end;

	with RankedEmployeeData as (
	select EmpId, EmpName, Salary, UpdateTimeFlag, isActive,
    Rank() Over (Partition By EmpId Order By UpdateTimeFlag desc) as ru
	From employee_trgt
	)

    merge into employee_trgt as target
    using(
		select src.EmpId, src.EmpName, src.Salary, src.UpdateTimeFlag
		from employee_src as src
		left join RankedEmployeeData as rd
		on src.EmpId = rd.EmpId and rd.ru = 1
	) as source
    on target.EmpId = source.EmpId
    
    when matched and source.UpdateTimeFlag <> target.UpdateTimeFlag then
		insert (EmpId, EmpName, Salary, UpdateTimeFlag, isActive)
		values (source.EmpId, source.EmpName, source.Salary, source.UpdateTimeFlag, 1)
    
    when not matched by target then
        insert (EmpId, EmpName, Salary, UpdateTimeFlag, isActive)
        values (source.EmpId, source.EmpName, source.Salary, source.UpdateTimeFlag, 1)
    
	when not matched by source and target.isActive = 1 then
		update set target.isActive = 0;
end;
go

exec SyncTable;
exec SyncTable @defaultFlag = 1;

insert into employee_src(EmpName,Salary) values('Viji', 90000);
update employee_src set salary = 9000 where EmpId=7;
delete from employee_src where EmpId=3;

select * from employee_src
select * from employee_trgt

 