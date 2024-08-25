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

exec sp_rename 'employee_trgt.isActive', 'isDeleted', 'COLUMN';
exec sp_rename 'employee_trgt.UpdateTimeFlag', 'UpdatedTime', 'COLUMN';
exec sp_rename 'employee_src.UpdateTimeFlag', 'UpdatedTime', 'COLUMN';

select * from employee_src
select * from employee_trgt
go

create or alter trigger trg_UpdateEmployeeSrc
on employee_src
after update
as
begin
    update src
    set UpdatedTime = GetDate()
    from employee_src src
    inner join inserted i on src.EmpId = i.EmpId;
end;
go

create or alter procedure SyncTable(@debugFlag int=0)
as
begin
	 if @debugFlag=1
		 select trgt.EmpId, trgt.EmpName, trgt.Salary, trgt.UpdatedTime, trgt.isDeleted from employee_trgt trgt
		 inner join(
			select EmpId, max(UpdatedTime) as MaxUpdatedTime from employee_trgt 
			group by EmpId
		 ) latest_trgt on trgt.EmpId = latest_trgt.EmpId and trgt.UpdatedTime = latest_trgt.MaxUpdatedTime order by EmpId;

	 insert into employee_trgt (EmpId, EmpName, Salary, UpdatedTime, isDeleted)
	 select src.EmpId, src.EmpName, src.Salary, src.UpdatedTime, 0
	 from employee_src src
	 left join (
        select EmpId, max(UpdatedTime) AS MaxUpdatedTime from employee_trgt group by EmpId
     )latest_trgt on src.EmpId = latest_trgt.EmpId
	 where latest_trgt.EmpId is null;

	 insert into employee_trgt (EmpId, EmpName, Salary, UpdatedTime, isDeleted)
	 select src.EmpId, src.EmpName, src.Salary, src.UpdatedTime, 0 from employee_src src
	 left join(
        select EmpId, max(UpdatedTime) as MaxUpdatedTime from employee_trgt group by EmpId
     )latest_trgt on src.EmpId = latest_trgt.EmpId
     where src.UpdatedTime <> latest_trgt.MaxUpdatedTime

	 update trgt set isDeleted = 1 from employee_trgt trgt
	 left join employee_src src on trgt.EmpId = src.EmpId
     where src.EmpId is null and trgt.isDeleted = 0;
end;
go

exec SyncTable;
exec SyncTable @debugFlag = 1;

insert into employee_src(EmpName,Salary) values('Roke', 90000);
update employee_src set salary = 80000 where EmpId=14;
delete from employee_src where EmpId=14;



/*create or alter procedure SyncTable(@defaultFlag int = 0)
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
*/



 