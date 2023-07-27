package com.tarento.nsdc.repo;

import com.tarento.nsdc.entity.CourseEntity;
import com.tarento.nsdc.entity.CourseEntityV2;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ICourseRepoV2 extends JpaRepository<CourseEntityV2, String> {

}
