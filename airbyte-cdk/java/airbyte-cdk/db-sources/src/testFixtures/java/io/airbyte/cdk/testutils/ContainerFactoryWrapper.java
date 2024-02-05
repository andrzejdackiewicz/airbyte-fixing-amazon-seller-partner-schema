/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.testutils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/**
 * This class wraps a specific shared testcontainer instance, which is created exactly once.
 */
class ContainerFactoryWrapper<C extends JdbcDatabaseContainer<?>> {

  static private final Logger LOGGER = LoggerFactory.getLogger(ContainerFactoryWrapper.class);
  static private final ConcurrentHashMap<String, ContainerFactoryWrapper<?>> SHARED_SINGLETONS = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  static <C extends JdbcDatabaseContainer<?>> C getOrCreateShared(ContainerFactory<C> factory, String imageName, String... methods) {
    final String mapKey = createMapKey(factory.getClass(), imageName, methods);
    final ContainerFactoryWrapper<?> singleton = SHARED_SINGLETONS.computeIfAbsent(mapKey, ContainerFactoryWrapper<C>::new);
    return ((ContainerFactoryWrapper<C>) singleton).getOrCreate(factory);
  }

  final String imageName;
  final List<String> methodNames;

  private C sharedContainer;
  private RuntimeException containerCreationError;

  private ContainerFactoryWrapper(String mapKey) {
    this.imageName = mapKeyElements(mapKey).skip(1).findFirst().get();
    this.methodNames = mapKeyElements(mapKey).skip(2).toList();
  }

  static private String createMapKey(Class<?> containerFactoryClass,  String imageName, String... methods) {
    final Stream<String> mapKeyElements = Stream.concat(Stream.of(containerFactoryClass.getCanonicalName(), imageName), Stream.of(methods));
    return mapKeyElements.collect(Collectors.joining("+"));
  }

  static private Stream<String> mapKeyElements(String mapKey) {
    return Arrays.stream(mapKey.split("\\+"));
  }

  private synchronized C getOrCreate(ContainerFactory<C> factory) {
    if (sharedContainer == null && containerCreationError == null) {
      try {
        create(imageName, factory, methodNames);
      } catch (RuntimeException e) {
        sharedContainer = null;
        containerCreationError = e;
      }
    }
    if (containerCreationError != null) {
      throw new RuntimeException(
          "Error during container creation for imageName=" + imageName
              + ", factory=" + factory.getClass().getName()
              + ", methods=" + methodNames,
          containerCreationError);
    }
    return sharedContainer;
  }

  private void create(String imageName, ContainerFactory<C> factory, List<String> methodNames) {
    LOGGER.info("Creating new shared container based on {} with {}.", imageName, methodNames);
    try {
      final var parsed = DockerImageName.parse(imageName);
      final var methods = new ArrayList<Method>();
      for (String methodName : methodNames) {
        methods.add(factory.getClass().getMethod(methodName, factory.getContainerClass()));
      }
      sharedContainer = factory.createNewContainer(parsed);
      sharedContainer.withLogConsumer(new Slf4jLogConsumer(LOGGER));
      for (Method method : methods) {
        LOGGER.info("Calling {} in {} on new shared container based on {}.",
            method.getName(), factory.getClass().getName(), imageName);
        method.invoke(factory, sharedContainer);
      }
      sharedContainer.start();
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

}
