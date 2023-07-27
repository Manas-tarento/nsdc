package com.tarento.nsdc.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import javax.persistence.*;
import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "coursesV2")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
public class CourseEntityV2 implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @Type(type = "jsonb")
    @Column(name = "json_data", columnDefinition = "jsonb")
    private JsonNode jsonData;
}
