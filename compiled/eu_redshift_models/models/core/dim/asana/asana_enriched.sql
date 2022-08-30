---Authors: Etoma Egot
---Last Modified by: Etoma Egot

---###################################################################################################################

        ---This asana model contains data from the following asana projects listed below:
        ---This model was created on 22.07.2022 as a core guide to aid with quantifying data team impact.
        -- This model will be enriched with additionally computed metrics and dimensions:
        -- 1. data asana project
        -- 2. 💻🔌 | IT Support
        -- 3. Data Democracy Project Backlog
        -- 4. Web Analytics - GA & GTM
        -- 5. FELD M: DWH & Dashboards 
        -- 6: Web Analytics Sprint Board
        -- 7. Data | Tactics planning  2022
        
    ---Note: This model can be used by all users who intend to extract insights from the aforementioned projects
---###################################################################################################################

 


with

-- ########################################
-- CREATING THE ASANA ENRICHED MODEL
-- ########################################
             project_task as (
				     
				select project_id, task_id
				       from "airup_eu_dwh"."asana"."project_task"
                         
				 ),
				  project as (
				     select id, name as project_name
				     from "airup_eu_dwh"."asana"."project"
				  ),
				  
				  task as (
				      select id, name as task_name,notes,
				      completed, created_at, completed_at, 
				      custom_impacted as Impacted, custom_team as team,
				      custom_priority as priority, custom_requestor as requestor,
				      task.assignee_id
				      from "airup_eu_dwh"."asana"."task"
				      
				  ), 
				    
				   users as (
				      select id, name as assignee, email as assignee_email
				      from "airup_eu_dwh"."asana"."user"
				   ),
				   				  
				  main as(
				  select 
				     project_task.project_id, project_task.task_id,project.id, project.project_name,
				     task.task_name,task.notes,task.completed, task.created_at, task.completed_at, task.impacted,
				     task.team, task.priority, task.requestor, task.assignee_id
				     from project_task 
				     left join project on project_task.project_id = project.id
				     left join task on project_task.task_id = task.id
				     
				  )
				 
				  select main.*, users.assignee, users.assignee_email
				  from main
				  left join users on main.assignee_id = users.id