import pytest
from pathlib import Path
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_table_does_not_exist,
    run_dbt_and_capture,
)


models__invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from {{ this.schema }}.seed

"""

models__table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

models__table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

models__view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed

"""

models__dep_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ref('view_model')}}

"""

models__view_with_conflicting_cascade_sql = """
select * from {{ref('table_a')}}

union all

select * from {{ref('table_b')}}

"""

models__skip_sql = """
select * from {{ref('invalid')}}

"""

seeds__seed_sql = """
create table {schema}.seed (
  id BIGSERIAL PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(50),
  gender VARCHAR(50),
  ip_address VARCHAR(20)
);


insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values
('Jack', 'Hunter', 'jhunter0@pbs.org', 'Male', '59.80.20.168'),
('Kathryn', 'Walker', 'kwalker1@ezinearticles.com', 'Female', '194.121.179.35'),
('Gerald', 'Ryan', 'gryan2@com.com', 'Male', '11.3.212.243'),
('Bonnie', 'Spencer', 'bspencer3@ameblo.jp', 'Female', '216.32.196.175'),
('Harold', 'Taylor', 'htaylor4@people.com.cn', 'Male', '253.10.246.136'),
('Jacqueline', 'Griffin', 'jgriffin5@t.co', 'Female', '16.13.192.220'),
('Wanda', 'Arnold', 'warnold6@google.nl', 'Female', '232.116.150.64'),
('Craig', 'Ortiz', 'cortiz7@sciencedaily.com', 'Male', '199.126.106.13'),
('Gary', 'Day', 'gday8@nih.gov', 'Male', '35.81.68.186'),
('Rose', 'Wright', 'rwright9@yahoo.co.jp', 'Female', '236.82.178.100'),
('Raymond', 'Kelley', 'rkelleya@fc2.com', 'Male', '213.65.166.67'),
('Gerald', 'Robinson', 'grobinsonb@disqus.com', 'Male', '72.232.194.193'),
('Mildred', 'Martinez', 'mmartinezc@samsung.com', 'Female', '198.29.112.5'),
('Dennis', 'Arnold', 'darnoldd@google.com', 'Male', '86.96.3.250'),
('Judy', 'Gray', 'jgraye@opensource.org', 'Female', '79.218.162.245'),
('Theresa', 'Garza', 'tgarzaf@epa.gov', 'Female', '21.59.100.54'),
('Gerald', 'Robertson', 'grobertsong@csmonitor.com', 'Male', '131.134.82.96'),
('Philip', 'Hernandez', 'phernandezh@adobe.com', 'Male', '254.196.137.72'),
('Julia', 'Gonzalez', 'jgonzalezi@cam.ac.uk', 'Female', '84.240.227.174'),
('Andrew', 'Davis', 'adavisj@patch.com', 'Male', '9.255.67.25'),
('Kimberly', 'Harper', 'kharperk@foxnews.com', 'Female', '198.208.120.253'),
('Mark', 'Martin', 'mmartinl@marketwatch.com', 'Male', '233.138.182.153'),
('Cynthia', 'Ruiz', 'cruizm@google.fr', 'Female', '18.178.187.201'),
('Samuel', 'Carroll', 'scarrolln@youtu.be', 'Male', '128.113.96.122'),
('Jennifer', 'Larson', 'jlarsono@vinaora.com', 'Female', '98.234.85.95'),
('Ashley', 'Perry', 'aperryp@rakuten.co.jp', 'Female', '247.173.114.52'),
('Howard', 'Rodriguez', 'hrodriguezq@shutterfly.com', 'Male', '231.188.95.26'),
('Amy', 'Brooks', 'abrooksr@theatlantic.com', 'Female', '141.199.174.118'),
('Louise', 'Warren', 'lwarrens@adobe.com', 'Female', '96.105.158.28'),
('Tina', 'Watson', 'twatsont@myspace.com', 'Female', '251.142.118.177'),
('Janice', 'Kelley', 'jkelleyu@creativecommons.org', 'Female', '239.167.34.233'),
('Terry', 'Mccoy', 'tmccoyv@bravesites.com', 'Male', '117.201.183.203'),
('Jeffrey', 'Morgan', 'jmorganw@surveymonkey.com', 'Male', '78.101.78.149'),
('Louis', 'Harvey', 'lharveyx@sina.com.cn', 'Male', '51.50.0.167'),
('Philip', 'Miller', 'pmillery@samsung.com', 'Male', '103.255.222.110'),
('Willie', 'Marshall', 'wmarshallz@ow.ly', 'Male', '149.219.91.68'),
('Patrick', 'Lopez', 'plopez10@redcross.org', 'Male', '250.136.229.89'),
('Adam', 'Jenkins', 'ajenkins11@harvard.edu', 'Male', '7.36.112.81'),
('Benjamin', 'Cruz', 'bcruz12@linkedin.com', 'Male', '32.38.98.15'),
('Ruby', 'Hawkins', 'rhawkins13@gmpg.org', 'Female', '135.171.129.255'),
('Carlos', 'Barnes', 'cbarnes14@a8.net', 'Male', '240.197.85.140'),
('Ruby', 'Griffin', 'rgriffin15@bravesites.com', 'Female', '19.29.135.24'),
('Sean', 'Mason', 'smason16@icq.com', 'Male', '159.219.155.249'),
('Anthony', 'Payne', 'apayne17@utexas.edu', 'Male', '235.168.199.218'),
('Steve', 'Cruz', 'scruz18@pcworld.com', 'Male', '238.201.81.198'),
('Anthony', 'Garcia', 'agarcia19@flavors.me', 'Male', '25.85.10.18'),
('Doris', 'Lopez', 'dlopez1a@sphinn.com', 'Female', '245.218.51.238'),
('Susan', 'Nichols', 'snichols1b@freewebs.com', 'Female', '199.99.9.61'),
('Wanda', 'Ferguson', 'wferguson1c@yahoo.co.jp', 'Female', '236.241.135.21'),
('Andrea', 'Pierce', 'apierce1d@google.co.uk', 'Female', '132.40.10.209'),
('Lawrence', 'Phillips', 'lphillips1e@jugem.jp', 'Male', '72.226.82.87'),
('Judy', 'Gilbert', 'jgilbert1f@multiply.com', 'Female', '196.250.15.142'),
('Eric', 'Williams', 'ewilliams1g@joomla.org', 'Male', '222.202.73.126'),
('Ralph', 'Romero', 'rromero1h@sogou.com', 'Male', '123.184.125.212'),
('Jean', 'Wilson', 'jwilson1i@ocn.ne.jp', 'Female', '176.106.32.194'),
('Lori', 'Reynolds', 'lreynolds1j@illinois.edu', 'Female', '114.181.203.22'),
('Donald', 'Moreno', 'dmoreno1k@bbc.co.uk', 'Male', '233.249.97.60'),
('Steven', 'Berry', 'sberry1l@eepurl.com', 'Male', '186.193.50.50'),
('Theresa', 'Shaw', 'tshaw1m@people.com.cn', 'Female', '120.37.71.222'),
('John', 'Stephens', 'jstephens1n@nationalgeographic.com', 'Male', '191.87.127.115'),
('Richard', 'Jacobs', 'rjacobs1o@state.tx.us', 'Male', '66.210.83.155'),
('Andrew', 'Lawson', 'alawson1p@over-blog.com', 'Male', '54.98.36.94'),
('Peter', 'Morgan', 'pmorgan1q@rambler.ru', 'Male', '14.77.29.106'),
('Nicole', 'Garrett', 'ngarrett1r@zimbio.com', 'Female', '21.127.74.68'),
('Joshua', 'Kim', 'jkim1s@edublogs.org', 'Male', '57.255.207.41'),
('Ralph', 'Roberts', 'rroberts1t@people.com.cn', 'Male', '222.143.131.109'),
('George', 'Montgomery', 'gmontgomery1u@smugmug.com', 'Male', '76.75.111.77'),
('Gerald', 'Alvarez', 'galvarez1v@flavors.me', 'Male', '58.157.186.194'),
('Donald', 'Olson', 'dolson1w@whitehouse.gov', 'Male', '69.65.74.135'),
('Carlos', 'Morgan', 'cmorgan1x@pbs.org', 'Male', '96.20.140.87'),
('Aaron', 'Stanley', 'astanley1y@webnode.com', 'Male', '163.119.217.44'),
('Virginia', 'Long', 'vlong1z@spiegel.de', 'Female', '204.150.194.182'),
('Robert', 'Berry', 'rberry20@tripadvisor.com', 'Male', '104.19.48.241'),
('Antonio', 'Brooks', 'abrooks21@unesco.org', 'Male', '210.31.7.24'),
('Ruby', 'Garcia', 'rgarcia22@ovh.net', 'Female', '233.218.162.214'),
('Jack', 'Hanson', 'jhanson23@blogtalkradio.com', 'Male', '31.55.46.199'),
('Kathryn', 'Nelson', 'knelson24@walmart.com', 'Female', '14.189.146.41'),
('Jason', 'Reed', 'jreed25@printfriendly.com', 'Male', '141.189.89.255'),
('George', 'Coleman', 'gcoleman26@people.com.cn', 'Male', '81.189.221.144'),
('Rose', 'King', 'rking27@ucoz.com', 'Female', '212.123.168.231'),
('Johnny', 'Holmes', 'jholmes28@boston.com', 'Male', '177.3.93.188'),
('Katherine', 'Gilbert', 'kgilbert29@altervista.org', 'Female', '199.215.169.61'),
('Joshua', 'Thomas', 'jthomas2a@ustream.tv', 'Male', '0.8.205.30'),
('Julie', 'Perry', 'jperry2b@opensource.org', 'Female', '60.116.114.192'),
('Richard', 'Perry', 'rperry2c@oracle.com', 'Male', '181.125.70.232'),
('Kenneth', 'Ruiz', 'kruiz2d@wikimedia.org', 'Male', '189.105.137.109'),
('Jose', 'Morgan', 'jmorgan2e@webnode.com', 'Male', '101.134.215.156'),
('Donald', 'Campbell', 'dcampbell2f@goo.ne.jp', 'Male', '102.120.215.84'),
('Debra', 'Collins', 'dcollins2g@uol.com.br', 'Female', '90.13.153.235'),
('Jesse', 'Johnson', 'jjohnson2h@stumbleupon.com', 'Male', '225.178.125.53'),
('Elizabeth', 'Stone', 'estone2i@histats.com', 'Female', '123.184.126.221'),
('Angela', 'Rogers', 'arogers2j@goodreads.com', 'Female', '98.104.132.187'),
('Emily', 'Dixon', 'edixon2k@mlb.com', 'Female', '39.190.75.57'),
('Albert', 'Scott', 'ascott2l@tinypic.com', 'Male', '40.209.13.189'),
('Barbara', 'Peterson', 'bpeterson2m@ow.ly', 'Female', '75.249.136.180'),
('Adam', 'Greene', 'agreene2n@fastcompany.com', 'Male', '184.173.109.144'),
('Earl', 'Sanders', 'esanders2o@hc360.com', 'Male', '247.34.90.117'),
('Angela', 'Brooks', 'abrooks2p@mtv.com', 'Female', '10.63.249.126'),
('Harold', 'Foster', 'hfoster2q@privacy.gov.au', 'Male', '139.214.40.244'),
('Carl', 'Meyer', 'cmeyer2r@disqus.com', 'Male', '204.117.7.88');
"""

seeds__update_sql = """
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values
('Michael', 'Perez', 'mperez0@chronoengine.com', 'Male', '106.239.70.175'),
('Shawn', 'Mccoy', 'smccoy1@reddit.com', 'Male', '24.165.76.182'),
('Kathleen', 'Payne', 'kpayne2@cargocollective.com', 'Female', '113.207.168.106'),
('Jimmy', 'Cooper', 'jcooper3@cargocollective.com', 'Male', '198.24.63.114'),
('Katherine', 'Rice', 'krice4@typepad.com', 'Female', '36.97.186.238'),
('Sarah', 'Ryan', 'sryan5@gnu.org', 'Female', '119.117.152.40'),
('Martin', 'Mcdonald', 'mmcdonald6@opera.com', 'Male', '8.76.38.115'),
('Frank', 'Robinson', 'frobinson7@wunderground.com', 'Male', '186.14.64.194'),
('Jennifer', 'Franklin', 'jfranklin8@mail.ru', 'Female', '91.216.3.131'),
('Henry', 'Welch', 'hwelch9@list-manage.com', 'Male', '176.35.182.168'),
('Fred', 'Snyder', 'fsnydera@reddit.com', 'Male', '217.106.196.54'),
('Amy', 'Dunn', 'adunnb@nba.com', 'Female', '95.39.163.195'),
('Kathleen', 'Meyer', 'kmeyerc@cdc.gov', 'Female', '164.142.188.214'),
('Steve', 'Ferguson', 'sfergusond@reverbnation.com', 'Male', '138.22.204.251'),
('Teresa', 'Hill', 'thille@dion.ne.jp', 'Female', '82.84.228.235'),
('Amanda', 'Harper', 'aharperf@mail.ru', 'Female', '16.123.56.176'),
('Kimberly', 'Ray', 'krayg@xing.com', 'Female', '48.66.48.12'),
('Johnny', 'Knight', 'jknighth@jalbum.net', 'Male', '99.30.138.123'),
('Virginia', 'Freeman', 'vfreemani@tiny.cc', 'Female', '225.172.182.63'),
('Anna', 'Austin', 'aaustinj@diigo.com', 'Female', '62.111.227.148'),
('Willie', 'Hill', 'whillk@mail.ru', 'Male', '0.86.232.249'),
('Sean', 'Harris', 'sharrisl@zdnet.com', 'Male', '117.165.133.249'),
('Mildred', 'Adams', 'madamsm@usatoday.com', 'Female', '163.44.97.46'),
('David', 'Graham', 'dgrahamn@zimbio.com', 'Male', '78.13.246.202'),
('Victor', 'Hunter', 'vhuntero@ehow.com', 'Male', '64.156.179.139'),
('Aaron', 'Ruiz', 'aruizp@weebly.com', 'Male', '34.194.68.78'),
('Benjamin', 'Brooks', 'bbrooksq@jalbum.net', 'Male', '20.192.189.107'),
('Lisa', 'Wilson', 'lwilsonr@japanpost.jp', 'Female', '199.152.130.217'),
('Benjamin', 'King', 'bkings@comsenz.com', 'Male', '29.189.189.213'),
('Christina', 'Williamson', 'cwilliamsont@boston.com', 'Female', '194.101.52.60'),
('Jane', 'Gonzalez', 'jgonzalezu@networksolutions.com', 'Female', '109.119.12.87'),
('Thomas', 'Owens', 'towensv@psu.edu', 'Male', '84.168.213.153'),
('Katherine', 'Moore', 'kmoorew@naver.com', 'Female', '183.150.65.24'),
('Jennifer', 'Stewart', 'jstewartx@yahoo.com', 'Female', '38.41.244.58'),
('Sara', 'Tucker', 'stuckery@topsy.com', 'Female', '181.130.59.184'),
('Harold', 'Ortiz', 'hortizz@vkontakte.ru', 'Male', '198.231.63.137'),
('Shirley', 'James', 'sjames10@yelp.com', 'Female', '83.27.160.104'),
('Dennis', 'Johnson', 'djohnson11@slate.com', 'Male', '183.178.246.101'),
('Louise', 'Weaver', 'lweaver12@china.com.cn', 'Female', '1.14.110.18'),
('Maria', 'Armstrong', 'marmstrong13@prweb.com', 'Female', '181.142.1.249'),
('Gloria', 'Cruz', 'gcruz14@odnoklassniki.ru', 'Female', '178.232.140.243'),
('Diana', 'Spencer', 'dspencer15@ifeng.com', 'Female', '125.153.138.244'),
('Kelly', 'Nguyen', 'knguyen16@altervista.org', 'Female', '170.13.201.119'),
('Jane', 'Rodriguez', 'jrodriguez17@biblegateway.com', 'Female', '12.102.249.81'),
('Scott', 'Brown', 'sbrown18@geocities.jp', 'Male', '108.174.99.192'),
('Norma', 'Cruz', 'ncruz19@si.edu', 'Female', '201.112.156.197'),
('Marie', 'Peters', 'mpeters1a@mlb.com', 'Female', '231.121.197.144'),
('Lillian', 'Carr', 'lcarr1b@typepad.com', 'Female', '206.179.164.163'),
('Judy', 'Nichols', 'jnichols1c@t-online.de', 'Female', '158.190.209.194'),
('Billy', 'Long', 'blong1d@yahoo.com', 'Male', '175.20.23.160'),
('Howard', 'Reid', 'hreid1e@exblog.jp', 'Male', '118.99.196.20'),
('Laura', 'Ferguson', 'lferguson1f@tuttocitta.it', 'Female', '22.77.87.110'),
('Anne', 'Bailey', 'abailey1g@geocities.com', 'Female', '58.144.159.245'),
('Rose', 'Morgan', 'rmorgan1h@ehow.com', 'Female', '118.127.97.4'),
('Nicholas', 'Reyes', 'nreyes1i@google.ru', 'Male', '50.135.10.252'),
('Joshua', 'Kennedy', 'jkennedy1j@house.gov', 'Male', '154.6.163.209'),
('Paul', 'Watkins', 'pwatkins1k@upenn.edu', 'Male', '177.236.120.87'),
('Kathryn', 'Kelly', 'kkelly1l@businessweek.com', 'Female', '70.28.61.86'),
('Adam', 'Armstrong', 'aarmstrong1m@techcrunch.com', 'Male', '133.235.24.202'),
('Norma', 'Wallace', 'nwallace1n@phoca.cz', 'Female', '241.119.227.128'),
('Timothy', 'Reyes', 'treyes1o@google.cn', 'Male', '86.28.23.26'),
('Elizabeth', 'Patterson', 'epatterson1p@sun.com', 'Female', '139.97.159.149'),
('Edward', 'Gomez', 'egomez1q@google.fr', 'Male', '158.103.108.255'),
('David', 'Cox', 'dcox1r@friendfeed.com', 'Male', '206.80.80.58'),
('Brenda', 'Wood', 'bwood1s@over-blog.com', 'Female', '217.207.44.179'),
('Adam', 'Walker', 'awalker1t@blogs.com', 'Male', '253.211.54.93'),
('Michael', 'Hart', 'mhart1u@wix.com', 'Male', '230.206.200.22'),
('Jesse', 'Ellis', 'jellis1v@google.co.uk', 'Male', '213.254.162.52'),
('Janet', 'Powell', 'jpowell1w@un.org', 'Female', '27.192.194.86'),
('Helen', 'Ford', 'hford1x@creativecommons.org', 'Female', '52.160.102.168'),
('Gerald', 'Carpenter', 'gcarpenter1y@about.me', 'Male', '36.30.194.218'),
('Kathryn', 'Oliver', 'koliver1z@army.mil', 'Female', '202.63.103.69'),
('Alan', 'Berry', 'aberry20@gov.uk', 'Male', '246.157.112.211'),
('Harry', 'Andrews', 'handrews21@ameblo.jp', 'Male', '195.108.0.12'),
('Andrea', 'Hall', 'ahall22@hp.com', 'Female', '149.162.163.28'),
('Barbara', 'Wells', 'bwells23@behance.net', 'Female', '224.70.72.1'),
('Anne', 'Wells', 'awells24@apache.org', 'Female', '180.168.81.153'),
('Harry', 'Harper', 'hharper25@rediff.com', 'Male', '151.87.130.21'),
('Jack', 'Ray', 'jray26@wufoo.com', 'Male', '220.109.38.178'),
('Phillip', 'Hamilton', 'phamilton27@joomla.org', 'Male', '166.40.47.30'),
('Shirley', 'Hunter', 'shunter28@newsvine.com', 'Female', '97.209.140.194'),
('Arthur', 'Daniels', 'adaniels29@reuters.com', 'Male', '5.40.240.86'),
('Virginia', 'Rodriguez', 'vrodriguez2a@walmart.com', 'Female', '96.80.164.184'),
('Christina', 'Ryan', 'cryan2b@hibu.com', 'Female', '56.35.5.52'),
('Theresa', 'Mendoza', 'tmendoza2c@vinaora.com', 'Female', '243.42.0.210'),
('Jason', 'Cole', 'jcole2d@ycombinator.com', 'Male', '198.248.39.129'),
('Phillip', 'Bryant', 'pbryant2e@rediff.com', 'Male', '140.39.116.251'),
('Adam', 'Torres', 'atorres2f@sun.com', 'Male', '101.75.187.135'),
('Margaret', 'Johnston', 'mjohnston2g@ucsd.edu', 'Female', '159.30.69.149'),
('Paul', 'Payne', 'ppayne2h@hhs.gov', 'Male', '199.234.140.220'),
('Todd', 'Willis', 'twillis2i@businessweek.com', 'Male', '191.59.136.214'),
('Willie', 'Oliver', 'woliver2j@noaa.gov', 'Male', '44.212.35.197'),
('Frances', 'Robertson', 'frobertson2k@go.com', 'Female', '31.117.65.136'),
('Gregory', 'Hawkins', 'ghawkins2l@joomla.org', 'Male', '91.3.22.49'),
('Lisa', 'Perkins', 'lperkins2m@si.edu', 'Female', '145.95.31.186'),
('Jacqueline', 'Anderson', 'janderson2n@cargocollective.com', 'Female', '14.176.0.187'),
('Shirley', 'Diaz', 'sdiaz2o@ucla.edu', 'Female', '207.12.95.46'),
('Nicole', 'Meyer', 'nmeyer2p@flickr.com', 'Female', '231.79.115.13'),
('Mary', 'Gray', 'mgray2q@constantcontact.com', 'Female', '210.116.64.253'),
('Jean', 'Mcdonald', 'jmcdonald2r@baidu.com', 'Female', '122.239.235.117');
"""


class BaseConcurrency:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.sql": seeds__seed_sql, "update.sql": seeds__update_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_a.sql": models__table_a_sql,
            "table_b.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": models__view_with_conflicting_cascade_sql,
            "skip.sql": models__skip_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.project_root / Path("seeds/seed.sql"))


class TestConcurenncy(BaseConcurrency):
    def test_concurrency(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7

        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        project.run_sql_file(project.project_root / Path("seeds/update.sql"))

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7

        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
