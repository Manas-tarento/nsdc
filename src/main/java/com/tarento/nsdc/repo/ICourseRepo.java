package com.tarento.nsdc.repo;

import com.tarento.nsdc.entity.CourseEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ICourseRepo extends JpaRepository<CourseEntity, String> {

}
