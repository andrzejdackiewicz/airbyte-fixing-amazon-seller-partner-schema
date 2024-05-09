/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb.state;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.bson.internal.UuidHelper;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import static java.util.Base64.getEncoder;

/**
 * _id field types that are currently supported, potential types are defined <a href=
 * "https://www.mongodb.com/docs/manual/reference/operator/query/type/#std-label-document-type-available-types">here</a>
 */
public enum IdType {

  OBJECT_ID("objectId", "ObjectId"),
  STRING("string", "String"),
  INT("int", "Integer"),
  LONG("long", "Long"),
  BINARY("binData", "Binary");

  private static final Map<String, IdType> byBsonType = new HashMap<>();
  static {
    for (final var idType : IdType.values()) {
      byBsonType.put(idType.bsonType.toLowerCase(), idType);
    }
  }

  private static final Map<String, IdType> byJavaType = new HashMap<>();
  static {
    for (final var idType : IdType.values()) {
      byJavaType.put(idType.javaType.toLowerCase(), idType);
    }
  }

  /** A comma-separated, human-readable list of supported _id types. */
  public static final String SUPPORTED;
  static {
    SUPPORTED = Arrays.stream(IdType.values())
        .map(e -> e.bsonType)
        .collect(Collectors.joining(", "));
  }

  /** MongoDb BSON type */
  private final String bsonType;
  /** Java class name type */
  private final String javaType;

  IdType(final String bsonType, final String javaType) {
    this.bsonType = bsonType;
    this.javaType = javaType;
  }

  public static Optional<IdType> findByBsonType(final String bsonType) {
    if (bsonType == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(byBsonType.get(bsonType.toLowerCase()));
  }

  public static Optional<IdType> findByJavaType(final String javaType) {
    if (javaType == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(byJavaType.get(javaType.toLowerCase()));
  }


  public static String idToStringRepresenation(final Object currentId, final IdType idType) {
    final String id;
    if (idType == IdType.BINARY) {
      final var binLastId = (Binary) currentId;
      if (binLastId.getType() == BsonBinarySubType.UUID_STANDARD.getValue()) {
        id = UuidHelper.decodeBinaryToUuid(binLastId.getData(), binLastId.getType(), UuidRepresentation.STANDARD).toString();
      } else {
        id = getEncoder().encodeToString(binLastId.getData());
      }
    } else {
      id = currentId.toString();
    }

    return id;
  }

  public static BsonBinary parseBinaryIdString(final String id) {
    try {
      return new BsonBinary(UUID.fromString(id));
    } catch (final IllegalArgumentException ex) {
      return new BsonBinary(Base64.getDecoder().decode(id));
    }
  }
}
