CREATE TABLE testlog (
    id          bigint not null,
    start_time  time,
    account_id  varchar(50),
    func_id     varchar(50),
    duration    float,
    retcode     varchar(10)
      CONSTRAINT pk PRIMARY KEY (id));
