package com.example.deltalake.controller;

import com.example.deltalake.DTO.StudentRequestDTO;
import com.example.deltalake.entities.Student;
import com.example.deltalake.service.StudentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.List;

@RestController
@RequestMapping("api/v1")
public class StudentController
{
    @Autowired
    private StudentService studentService;

    @PostMapping("/insertData")
    public void insertData() {
        studentService.insertData();
    }

    @GetMapping("/Students")
    public List<Student> getAllStudents() {

        return studentService.getAllStudents();
    }

    @GetMapping("/Students/{firstname}")
    public List<Student> getStudentsByFirstname(@RequestParam String firstname) {
        return studentService.getStudentsByFirstname(firstname);
    }

    @PostMapping("/Students")
    public void createProduit(@RequestBody StudentRequestDTO studentRequestDTO) {
        studentService.createStudent(studentRequestDTO);
    }
    @GetMapping("/Students/snapshot/before/{date}")
    public List<Student> getEtudiantsByVersionBeforeOrAtTimestamp(String date) throws ParseException {
        return studentService.getStudentsByVersionBeforeOrAtTimestamp(date);
    }
    @GetMapping("/Students/snapshot/after/{date}")
    public List<Student> getEtudiantsByVersionAfterOrAtTimestamp(String date) throws ParseException {
        return studentService.getStudentssByVersionAfterOrAtTimestamp(date);
    }
    @GetMapping("/Students/snapshot/version/{version}")
    public List<Student> getEtudiantsBySnapshotVersion(@RequestParam int version)
    {
        return studentService.getStudentssBySnapshotVersion(version);
    }
    @PutMapping("/students/{id}")
    public Student updateStudentById(@RequestParam Integer id, @RequestBody StudentRequestDTO studentRequestDTO)
    {
        return studentService.updateStudent(id, studentRequestDTO);
    }
    @DeleteMapping("/students/{id}")
    void deleteStudentById(Integer id)
    {
        studentService.deleteStudent(id);
    }
    @DeleteMapping("/students")
    void deleteStudens()
    {
        studentService.deleteAllStudents();
    }
}
