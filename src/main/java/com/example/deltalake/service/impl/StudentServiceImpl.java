package com.example.deltalake.service.impl;


import com.example.deltalake.DTO.StudentRequestDTO;
import com.example.deltalake.config.SparkSessionPool;
import com.example.deltalake.entities.Student;
import com.example.deltalake.service.StudentService;
import com.example.deltalake.utils.DateUtil;
import io.delta.standalone.DeltaLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class StudentServiceImpl implements StudentService
{
    private final SparkSessionPool sparkSessionPool;
    @Value("${delta.tables.studentPath}")
    private String PathStudentTable;


    @Autowired
    public StudentServiceImpl(SparkSessionPool sparkSessionPool) {
        this.sparkSessionPool = sparkSessionPool;
    }

    @Override
    @Transactional(readOnly = true)
    public List<Student> getAllStudents() {
        SparkSession sparkSession = null;
        try {
             sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Student> df = sparkSession.read().format("delta").load(PathStudentTable).as(Encoders.bean(Student.class));

            return df.collectAsList();
        } catch (Exception e) {
            log.error("Failed to load data from Delta table", e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Student> getStudentsByFirstname(String firstName) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Load data from a file
            Dataset<Student> df = sparkSession.read().format("delta").load(PathStudentTable).as(Encoders.bean(Student.class));
            df.createOrReplaceTempView("students");
            Dataset<Student> sqlDF = sparkSession.sql("SELECT * FROM students WHERE first_name ='"+firstName+"'").as(Encoders.bean(Student.class));

            return sqlDF.collectAsList();
        } catch (Exception e) {
            log.error("Failed to load data from Delta table", e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public Student createStudent(StudentRequestDTO studentRequestDTO) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> deltaDF = sparkSession.read().format("delta").load(PathStudentTable);
            Object maxIdObj = deltaDF.agg(functions.max("id")).head().get(0);
            Integer maxId = maxIdObj != null ? ((Integer) maxIdObj) : 0;
            Integer newId = ++maxId;
            Student student = new Student(newId, studentRequestDTO.getFirst_name(), studentRequestDTO.getLast_name());

            Dataset<Row> StudentDf = sparkSession.createDataFrame(Collections.singletonList(student), Student.class);
            StudentDf.write().format("delta").mode("append").save(PathStudentTable);
            return student;
        } catch (Exception e) {
            log.error("Failed to write data to Delta table", e);
            throw new RuntimeException("Failed to write data to Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional
    public void insertData() {
        SparkSession sparkSession = null;
        List<Student> students = new ArrayList();
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            // Create dataset of names and surnames
            String[] names = {"Antoine", "Gayleen", "Husein", "Janka", "Ibby", "Yuri", "Delmor", "Reba", "Tybalt", "Elke", "Yetta", "Nedda", "Emile", "Marga", "Nara", "Antonia", "Ely", "Sauveur", "Tomaso", "Pansie", "Tymon", "Renard", "Alfie", "Cacilie", "Graham", "Keir", "Elihu", "Leann", "Starla", "Kameko", "Ahmed", "Dana", "Micah", "Lethia", "Rawley", "Kylie", "Kameko", "Rebecca", "Nicolis", "Arda", "Adina", "Eal", "Rickert", "Aurlie", "Padgett", "Andros", "Eyde", "Gaspard", "Amie", "Kassia", "Vivia", "Dale", "Wilbert", "Linc", "Kele", "Christen", "Liza", "Genny", "Alayne", "Liesa", "Fran", "Clare", "Lonnie", "Darren", "Wang", "Jenny", "Lay", "Jackson", "Cookie", "Lira", "Carolee", "Sarene", "Lucho", "Raphael", "Cathyleen", "Juieta", "Hal", "Marissa", "Pepito", "Jasen", "Maybelle", "Gal", "Haze", "Kore", "Milicent", "Cirstoforo", "Malinde", "Aurelia", "Sansone", "Mendie", "Jami", "Lilia", "Dasha", "Filmore", "Halette", "Taddeusz", "Brander", "Lyda", "Renado", "Haze"};
            String[] surnames = {"Whelpdale", "Courtese", "Presman", "Halle", "Glanvill", "Laetham", "Derbyshire", "Agron", "Ferry", "Goldie", "Doige", "Marin", "Simon", "Airs", "Forge", "Kirkpatrick", "Rist", "Limpertz", "Gounod", "Pentlow", "Kippins", "Waldock", "Haworth", "Yanov", "Calton", "Ovens", "Hullot", "Dredge", "Worland", "Kmicicki", "Fairlie", "McIntosh", "Assinder", "Rhucroft", "Gooderson", "Ghidoni", "Pipes", "Lempertz", "Speak", "Densumbe", "Standbrook", "Crosbie", "Cutchey", "Shelford", "Howel", "Sitford", "Stannislawski", "Diano", "Luther", "Flanaghan", "Redman", "Clemerson", "Tidcombe", "Kingsmill", "Halsey", "Calafato", "Arrundale", "Newberry", "Dale", "Wilshaw", "Drakeford", "Jecock", "Dowd", "Eveling", "Swateridge", "Garvey", "Nan Carrow", "Worcester", "Tebboth", "Gommery", "Laurens", "Cocklin", "Dargavel", "Bloy", "Siddens", "Arend", "Sapauton", "Goodin", "Fogg", "Cejka", "Albutt", "Lomaz", "Arrington", "Hughland", "Tash", "Thick", "Mandal", "Ferschke", "Douse", "Willder", "Staveley", "De Andreis", "Baggiani", "Drennan", "Cogswell", "Merrikin", "Dregan", "Breton", "Broster", "Frays"};
            System.out.println(names.length);
            System.out.println(surnames.length);
            Dataset<Row> deltaDF = sparkSession.read().format("delta").load(PathStudentTable);
            Object maxIdObj = deltaDF.agg(functions.max("id")).head().get(0);
            Integer maxId = maxIdObj != null ? ((Integer) maxIdObj) : 0;
            for (int i = 0; i < 100; i++) {
                Student student = new Student(++maxId, names[i], surnames[i]);
                students.add(student);
            }
            Dataset<Row> StudentDf = sparkSession.createDataFrame(students, Student.class);
            StudentDf.write().format("delta").mode("append").save(PathStudentTable);
        } catch (Exception e) {
            log.error("Failed to write data to Delta table", e);
            throw new RuntimeException("Failed to write data to Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public Student updateStudent(Integer id, StudentRequestDTO studentRequestDTO)
    {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> students = sparkSession.read().format("delta").load(PathStudentTable);
            students.createOrReplaceTempView("students");
            sparkSession.sql("UPDATE students SET first_name = '" + studentRequestDTO.getFirst_name() + "', last_name = '" + studentRequestDTO.getLast_name() + "' WHERE id = " + id);
            return new Student(id,studentRequestDTO.getFirst_name(), studentRequestDTO.getLast_name());
        } catch (Exception e) {
            log.error("Failed update student with id = "+id, e);
            throw new RuntimeException("Failed update student with id = "+id, e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional
    public void deleteStudent(Integer id)
    {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> students = sparkSession.read().format("delta").load(PathStudentTable);
            students.createOrReplaceTempView("students");
            sparkSession.sql("DELETE FROM students WHERE id = " + id);
        } catch (Exception e) {
            log.error("Failed delete student with id = "+id, e);
            throw new RuntimeException("Failed delete student with id = "+id, e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }
    @Override
    @Transactional
    public void deleteAllStudents()
    {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Row> students = sparkSession.read().format("delta").load(PathStudentTable);
            students.createOrReplaceTempView("students");
            sparkSession.sql("DELETE FROM students");
        } catch (Exception e) {
            log.error("Failed delete all students", e);
            throw new RuntimeException("Failed delete all students", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Student> getStudentsByVersionBeforeOrAtTimestamp(String date){

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), PathStudentTable);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionBeforeOrAtTimestamp(Timestamp);
            Dataset<Student> df = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(PathStudentTable).as(Encoders.bean(Student.class));

            return df.collectAsList();
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } catch (Exception e) {
            log.error("Error getting students by version before or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Student> getStudentssByVersionAfterOrAtTimestamp(String date){

        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            DeltaLog log = DeltaLog.forTable(new Configuration(), PathStudentTable);
            long Timestamp = DateUtil.convertStringDateToLong(date);
            long snapshotVersion = log.getVersionAtOrAfterTimestamp(Timestamp);
            Dataset<Student> df = sparkSession.read().format("delta").option("versionAsOf", snapshotVersion).load(PathStudentTable).as(Encoders.bean(Student.class));

            return df.collectAsList();
        } catch (ParseException e) {
            log.error("Error parsing date: " + date, e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } catch (Exception e) {
            log.error("Error getting students by version after or at timestamp: " + e.getMessage(), e);
            throw new RuntimeException("Failed to load data from Delta table", e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<Student> getStudentssBySnapshotVersion(Integer version) {
        SparkSession sparkSession = null;
        try {
            sparkSession = sparkSessionPool.borrowSparkSession();
            Dataset<Student> df = sparkSession.read().format("delta").option("versionAsOf", version).load(PathStudentTable).as(Encoders.bean(Student.class));
            return df.collectAsList();
        } catch (Exception e) {
            log.error("Error getting students by version: " + e.getMessage(), e);
        } finally {
            if (sparkSession != null) {
                sparkSessionPool.returnSparkSession(sparkSession);
            }
        }
        return Collections.emptyList();
    }
}
