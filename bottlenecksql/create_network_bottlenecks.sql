CREATE TABLE bottlenecks (
tmc                  character varying(9),
start_epoch          smallint,            
end_epoch            smallint,            
year                 smallint,            
month                smallint,            
peak_severity        real,                
peak_epoch           smallint,            
peak_depth           smallint,            
hours_of_delay_dist  real[],              
traffic_volume_dist  real[],
state                CHAR(2)
);

