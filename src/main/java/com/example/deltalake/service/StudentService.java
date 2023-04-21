package com.example.deltalake.service;

import com.example.deltalake.DTO.StudentRequestDTO;
import com.example.deltalake.entities.Student;

import java.text.ParseException;
import java.util.List;

public interface StudentService
{
    void insertData();
    List<Student> getAllStudents();
    List<Student> getStudentsByFirstname(String firstName);
    Student createStudent(StudentRequestDTO studentRequestDTO);
    Student updateStudent(Integer id, StudentRequestDTO studentRequestDTO);
    void deleteStudent(Integer id);
    void deleteAllStudents();
    List<Student> getStudentsByVersionBeforeOrAtTimestamp(String date) throws ParseException;
    List<Student> getStudentssByVersionAfterOrAtTimestamp(String date) throws ParseException;
    List<Student> getStudentssBySnapshotVersion(Integer version);
}
