/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.scheduler.persistence;

import com.google.common.base.Preconditions;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.JobTypeResourceLimit;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.ResourceRequirements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ResourceRequirementsUtils {

  /**
   * Given connection-level resource requirements, actor-definition-level resource requirements,
   * worker-default resource requirements, and a job type, returns the final resource requirements
   * generated by merging the provided requirements in hierarchy order.
   *
   * Connection-level resource requirements take precendence over actor-definition level resource
   * requirements. Within the actor-definition level requirements, job-type-specific requirements take
   * precedence over default definition requirements. Actor-definition level resource requirements
   * take precedence over worker default resource requirements.
   *
   * @param connectionResourceReqs - the resource requirements set on the connection
   * @param actorDefinitionResourceReqs - the resource requirements set on the actor definition
   * @param workerDefaultResourceReqs - the default worker resource requirements set in the env
   *        variables
   * @param jobType - type of job to extract resource requirements for from the actor definition reqs
   * @return resource requirements, if present, otherwise an empty ResourceRequirements object.
   */
  public static ResourceRequirements getResourceRequirements(@Nullable final ResourceRequirements connectionResourceReqs,
                                                             @Nullable final ActorDefinitionResourceRequirements actorDefinitionResourceReqs,
                                                             @Nullable final ResourceRequirements workerDefaultResourceReqs,
                                                             final JobType jobType) {
    final ResourceRequirements jobSpecificDefinitionResourceReqs = getResourceRequirementsForJobType(actorDefinitionResourceReqs, jobType)
        .orElse(null);
    final ResourceRequirements defaultDefinitionResourceReqs = Optional.ofNullable(actorDefinitionResourceReqs)
        .map(ActorDefinitionResourceRequirements::getDefault).orElse(null);
    return mergeResourceRequirements(
        connectionResourceReqs,
        jobSpecificDefinitionResourceReqs,
        defaultDefinitionResourceReqs,
        workerDefaultResourceReqs);
  }

  /**
   * Given connection-level and worker-default resource requirements, returns the final resource
   * requirements generated by merging the provided requirements in hierarchy order.
   *
   * Connection-level resource requirements take precendence over worker-default resource
   * requirements.
   *
   * @param connectionResourceReqs - the resource requirements set on the connection
   * @param workerDefaultResourceReqs - the default worker resource requirements set in the env
   *        variables
   * @return resource requirements, if present, otherwise an empty ResourceRequirements object.
   */
  public static ResourceRequirements getResourceRequirements(@Nullable final ResourceRequirements connectionResourceReqs,
                                                             @Nullable final ResourceRequirements workerDefaultResourceReqs) {
    return mergeResourceRequirements(
        connectionResourceReqs,
        workerDefaultResourceReqs);
  }

  /**
   * Given a list of resource requirements, merges them together. Earlier reqs override later ones.
   *
   * @param resourceReqs - list of resource request to merge
   * @return merged resource req
   */
  private static ResourceRequirements mergeResourceRequirements(final ResourceRequirements... resourceReqs) {
    final ResourceRequirements outputReqs = new ResourceRequirements();
    final List<ResourceRequirements> reversed = new ArrayList<>(Arrays.asList(resourceReqs));
    Collections.reverse(reversed);

    // start from the lowest priority requirements so that we can repeatedly override the output
    // requirements to guarantee that we end with the highest priority setting for each
    for (final ResourceRequirements resourceReq : reversed) {
      if (resourceReq == null) {
        continue;
      }

      if (resourceReq.getCpuRequest() != null) {
        outputReqs.setCpuRequest(resourceReq.getCpuRequest());
      }
      if (resourceReq.getCpuLimit() != null) {
        outputReqs.setCpuLimit(resourceReq.getCpuLimit());
      }
      if (resourceReq.getMemoryRequest() != null) {
        outputReqs.setMemoryRequest(resourceReq.getMemoryRequest());
      }
      if (resourceReq.getMemoryLimit() != null) {
        outputReqs.setMemoryLimit(resourceReq.getMemoryLimit());
      }
    }
    return outputReqs;
  }

  private static Optional<ResourceRequirements> getResourceRequirementsForJobType(final ActorDefinitionResourceRequirements actorDefResourceReqs,
                                                                                  final JobType jobType) {
    if (actorDefResourceReqs == null) {
      return Optional.empty();
    }

    final List<ResourceRequirements> jobTypeResourceRequirement = actorDefResourceReqs.getJobSpecific()
        .stream()
        .filter(jobSpecific -> jobSpecific.getJobType() == jobType).map(JobTypeResourceLimit::getResourceRequirements).collect(
            Collectors.toList());

    Preconditions.checkArgument(jobTypeResourceRequirement.size() <= 1, "Should only have one resource requirement per job type.");
    return jobTypeResourceRequirement.isEmpty()
        ? Optional.empty()
        : Optional.of(jobTypeResourceRequirement.get(0));
  }

}
