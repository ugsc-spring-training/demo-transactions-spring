package com.example.demotransactionsspring;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Configuration
@ComponentScan
@EnableTransactionManagement
@EnableAsync
public class DemoTransactionsSpringApplication implements CommandLineRunner {
    @Autowired
    private PersonService personService;

    @Autowired
    private Client1 client1;

    @Autowired
    private Client2 client2;

    public static void main(String[] args) {
        SpringApplication.run(DemoTransactionsSpringApplication.class, args);
    }

    @Bean
    public DataSource dataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl("jdbc:sqlserver://localhost:1433;database=test");
        dataSource.setUsername("michal");
        dataSource.setPassword("password");
        return dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Override
    public void run(String... args) throws Exception {
//        client1.doSth();
//        client2.doSth();
        personService.doSth();
        System.out.println(personService.findAll());

    }
}

@Component
class Client1 {
    private final PersonService personService;

    Client1(PersonService personService) {
        this.personService = personService;
    }

    @Async
    public void doSth() {
        System.out.println("client1 thread ID: " + Thread.currentThread().getId());
        System.out.println(personService.findAll());
    }
}

@Component
class Client2 {
    private final PersonService personService;

    Client2(PersonService personService) {
        System.out.println("client2 thread ID: " + Thread.currentThread().getId());
        this.personService = personService;
    }

    @Async
    public void doSth() {
        personService.save(new Person(null, "total brutal novy Client2"));
        personService.save(new Person(null, "serialized  II"));
    }
}

@Service
class SecondService {
    private final PersonRepository personRepository;
    private final PlatformTransactionManager transactionManager;

    SecondService(PersonRepository personRepository, PlatformTransactionManager transactionManager) {
        this.personRepository = personRepository;
        this.transactionManager = transactionManager;
    }

    //    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void save(Person person) throws MyException {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(Propagation.REQUIRES_NEW.value());

        template.execute(action -> {
            try {
                personRepository.save(person);

                if (true) {
                    throw new RuntimeException("haha");
                }

            } catch(Exception e){
                action.setRollbackOnly();
            }

                return null;
            });
        }
    }

    class MyException extends Exception {
        public MyException(String message) {
            super(message);
        }
    }

    @Service
    class PersonService {
        private final PersonRepository personRepository;
        private final SecondService secondService;

        PersonService(PersonRepository personRepository, SecondService secondService) {
            this.personRepository = personRepository;
            this.secondService = secondService;
        }

        @Transactional
        public void doSth() {
            personRepository.save(new Person(null, "new"));
            try {
                secondService.save(new Person(null, "runtime exception"));
            } catch (Exception e) {
                System.out.println("Exception catched: " + e.getMessage());
            }

//        if (true) {
//            throw new RuntimeException();
//        }

//        personRepository.save(new Person(22L, "Tarzan 22"));
//        System.out.println(personRepository.findAll());
        }

        @Transactional
        public List<Person> findAll() {
            List<Person> all = personRepository.findAll();

//        try {
//            Thread.currentThread().sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

            return all;
        }

        @Transactional(timeout = 2)
        public void save(Person person) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            personRepository.save(person);


            System.out.println("save comitted!");
        }
    }

    @Repository
    class PersonRepository {
        private JdbcTemplate jdbcTemplate;

        public PersonRepository(DataSource dataSource) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        public List<Person> findAll() {
            return jdbcTemplate.query("select * from Person", new RowMapper<Person>() {
                @Override
                public Person mapRow(ResultSet resultSet, int i) throws SQLException {
                    return new Person(resultSet.getLong("id"), resultSet.getString("name"));
                }
            });
        }

        public void save(Person person) {
            if (person.getId() == null) {
                jdbcTemplate.update("insert into person (name) values (?)", person.getName());
            } else {
                jdbcTemplate.update("update person set name = ? where id = ?", person.getName(), person.getId());
            }
        }
    }


    class Person {
        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }

        public Person(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
