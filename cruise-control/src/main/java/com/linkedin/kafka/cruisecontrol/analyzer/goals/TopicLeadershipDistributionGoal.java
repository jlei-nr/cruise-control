/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;

/**
 * A hard goal that generates leadership movement and leader replica movement proposals to ensure that the number of
 * leader replicas on each non-excluded broker in the cluster is at most +1 for every topic (not every topic partition
 * count is perfectly divisible by every eligible broker count).
 */
public class TopicLeadershipDistributionGoal extends AbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(TopicLeadershipDistributionGoal.class);

    // We select randomly between proposed legal moves, but we cannot determine if we're violating any previously-ran
    // goals' action acceptances. This means we may end up in a temporarily stuck state where no legal moves can be
    // found for now but can given another set of randomly-selected moves.
    //
    // These two properties place a cap on the number of attempts so we do not end up in an infinite loop for instances
    // where no solution can be found.
    private static final int MAX_NUM_NO_MOVE_ITERATIONS = 10;
    private int _numNoMoveIterations = 0;

    private Set<String> _allowedTopics;
    private Set<Integer> _allowedBrokerIds;

    private final Map<String, Integer> _targetNumLeadReplicasByTopic;
    private final Map<String, Map<Integer, Integer>> _numLeadReplicasByTopicByBrokerId;
    private final Map<String, Integer> _latestNumMovesFoundByTopic = new HashMap<>();

    public TopicLeadershipDistributionGoal() {
        _targetNumLeadReplicasByTopic = new HashMap<>();
        _numLeadReplicasByTopicByBrokerId = new HashMap<>();
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        String topic = action.topic();
        Integer targetNumLeadReplicas = _targetNumLeadReplicasByTopic.get(topic);

        Replica replica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());

        Integer sourceNumLeadReplicas = getUncachedNumLeadReplicas(clusterModel, action.sourceBrokerId(), topic);
        if (replica.isLeader()) {
            sourceNumLeadReplicas -= 1;
        }

        Integer destinationNumLeadReplicas = getUncachedNumLeadReplicas(clusterModel, action.destinationBrokerId(), topic);
        if (replica.isLeader()) {
            destinationNumLeadReplicas += 1;
        }

        if (
                (sourceNumLeadReplicas.equals(targetNumLeadReplicas)
                        || sourceNumLeadReplicas.equals(targetNumLeadReplicas + 1))
                && (destinationNumLeadReplicas.equals(targetNumLeadReplicas)
                        || destinationNumLeadReplicas.equals(targetNumLeadReplicas + 1))
        ) {
            return ActionAcceptance.ACCEPT;
        } else {
            return ActionAcceptance.REPLICA_REJECT;
        }
    }

    private int getUncachedNumLeadReplicas(ClusterModel clusterModel, int brokerId, String topic) {
        return Math.toIntExact(
                clusterModel.getPartitionsByTopic().get(topic).stream()
                        .filter(partition -> partition.leader().broker().id() == brokerId).count()
        );
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new GoalUtils.HardGoalStatsComparator();
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
    }

    @Override
    public String name() {
        return TopicLeadershipDistributionGoal.class.getSimpleName();
    }

    @Override
    public boolean isHardGoal() {
        return true;
    }

    // The selfSatisfied check is a less strict version of the actionAcceptance check. The selfSatisfied check is happy
    // as long as we're trending in the correct direction whereas the actionAcceptance check makes sure an
    // already-fulfilled goal isn't then violated by a following action.
    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
        String topic = action.topic();
        Integer targetNumLeadReplicas = _targetNumLeadReplicasByTopic.get(topic);

        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
        Replica replica = sourceBroker.replica(action.topicPartition());

        Map<Integer, Integer> numLeadReplicasByBrokerId = _numLeadReplicasByTopicByBrokerId.get(topic);

        Integer sourceNumLeadReplicas = numLeadReplicasByBrokerId.getOrDefault(sourceBroker.id(), 0);
        Integer destinationNumLeadReplicas = numLeadReplicasByBrokerId.getOrDefault(destinationBroker.id(), 0);

        return (!replica.isLeader() || (sourceNumLeadReplicas >= targetNumLeadReplicas))
                && (!replica.isLeader() || (destinationNumLeadReplicas <= targetNumLeadReplicas + 1));
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        _allowedTopics = GoalUtils.topicsToRebalance(clusterModel, optimizationOptions.excludedTopics());

        if (_allowedTopics.isEmpty()) {
            LOG.warn("All topics are excluded from {}.", name());
        }

        Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForLeadership();
        _allowedBrokerIds = clusterModel.aliveBrokers().stream()
                .map(Broker::id)
                .filter(b -> !excludedBrokers.contains(b))
                .collect(Collectors.toCollection(HashSet::new));

        if (_allowedBrokerIds.isEmpty()) {
            throw new OptimizationFailureException(
                    "Cannot take any action as all alive brokers are excluded from leadership.");
        }

        _numNoMoveIterations = 0;

        _targetNumLeadReplicasByTopic.clear();
        _numLeadReplicasByTopicByBrokerId.clear();

        SortedMap<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();

        for (String topic : clusterModel.topics()) {
            // Each partition has one leader so the # of leaders is the same as the # of partitions.
            int numLeadReplicas = partitionsByTopic.get(topic).size();
            int targetNumLeadReplicas = Math.floorDiv(numLeadReplicas, _allowedBrokerIds.size());
            _targetNumLeadReplicasByTopic.put(topic, targetNumLeadReplicas);

            Map<Integer, Integer> numLeadReplicasPerBroker = new HashMap<>();
            for (Partition partition : partitionsByTopic.get(topic)) {
                numLeadReplicasPerBroker.compute(partition.leader().broker().id(), (k, v) -> v == null ? 1 : v + 1);
            }
            _numLeadReplicasByTopicByBrokerId.put(topic, numLeadReplicasPerBroker);

            LOG.info("Targeting {}(+1) lead replicas per broker for topic {}", targetNumLeadReplicas, topic);
        }
    }

    @Override
    protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        Set<String> imbalancedTopics = new HashSet<>();

        for (Map.Entry<String, Map<Integer, Integer>> entry : _numLeadReplicasByTopicByBrokerId.entrySet()) {
            Integer targetNumLeadReplicas = _targetNumLeadReplicasByTopic.get(entry.getKey());

            long numImbalanced = clusterModel.aliveBrokers().stream()
                    .map(Broker::id)
                    .filter(brokerId -> {
                        Integer numLeadReplicas = entry.getValue().getOrDefault(brokerId, 0);
                        return !numLeadReplicas.equals(targetNumLeadReplicas)
                                && !numLeadReplicas.equals(targetNumLeadReplicas + 1);
                    })
                    .count();

            if (numImbalanced > 0) {
                imbalancedTopics.add(entry.getKey());
            }
        }

        if (imbalancedTopics.isEmpty()) {
            finish();
        } else {
            _succeeded = false;
            checkIfSolutionIsPossible(imbalancedTopics);
        }
    }

    @Override
    protected void rebalanceForBroker(
            Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        if (!_allowedBrokerIds.contains(broker.id())) {
            return;
        }

        for (String topic : _allowedTopics) {
            Collection<Replica> topicReplicas = broker.replicasOfTopicInBroker(topic);
            Collection<Replica> leaderReplicas = topicReplicas.stream()
                    .filter(Replica::isLeader)
                    .collect(Collectors.toSet());

            int numLeadReplicasToMove = _targetNumLeadReplicasByTopic.get(topic) - leaderReplicas.size();
            int numMovesFound = 0;

            if (numLeadReplicasToMove > 0) {
                numMovesFound = gainLeadReplicas(
                        broker,
                        topic,
                        numLeadReplicasToMove,
                        clusterModel,
                        optimizedGoals,
                        optimizationOptions);
            } else if (numLeadReplicasToMove < -1) {
                numMovesFound = loseLeadReplicas(
                        broker,
                        topic,
                        numLeadReplicasToMove,
                        leaderReplicas,
                        clusterModel,
                        optimizedGoals,
                        optimizationOptions);
            }

            _latestNumMovesFoundByTopic.put(topic, numMovesFound);
        }
    }

    /**
     * If no moves were found for any topics, then this algorithm will be unable to ever find a solution. This check
     * will detect this situation and throw an {@link OptimizationFailureException} if we get stuck repeatedly.
     *
     * @throws OptimizationFailureException thrown if a solution is not possible
     */
    private void checkIfSolutionIsPossible(Set<String> imbalancedTopics)
            throws OptimizationFailureException {
        int numTopics = _latestNumMovesFoundByTopic.keySet().size();
        int numTopicsWithNoMovesFound = 0;

        for (int numMovesFound : _latestNumMovesFoundByTopic.values()) {
            if (numMovesFound == 0) {
                numTopicsWithNoMovesFound++;
            }
        }

        if (numTopicsWithNoMovesFound == numTopics) {
            StringBuilder s = new StringBuilder();

            List<String> sortedImbalancedTopics = new ArrayList<>(imbalancedTopics);
            sortedImbalancedTopics.sort(String::compareTo);

            for (String topic : sortedImbalancedTopics) {
                s.append(String.format(
                        "%n\t%s leadership distribution (expected count per broker: %s (+0/+1)):",
                        topic,
                        _targetNumLeadReplicasByTopic.get(topic)
                ));

                Map<Integer, Integer> numLeadReplicasByBroker = _numLeadReplicasByTopicByBrokerId.get(topic);

                List<Integer> brokerIds = new ArrayList<>(numLeadReplicasByBroker.keySet());
                brokerIds.sort(Integer::compareTo);

                for (Integer brokerId : brokerIds) {
                    int numLeadReplicas = numLeadReplicasByBroker.get(brokerId);
                    s.append(String.format("%n\t\t%s: %s", brokerId, numLeadReplicas));
                }
            }

            String errorMessage = "No solution found. Remaining topics with imbalanced leadership:" + s.toString();

            if (_numNoMoveIterations++ > MAX_NUM_NO_MOVE_ITERATIONS) {
                throw new OptimizationFailureException(errorMessage);
            } else {
                LOG.warn(errorMessage);
            }
        }
    }

    private int gainLeadReplicas(
            Broker broker,
            String topic,
            int numLeadReplicasToMove,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) {
        Collection<Replica> followerReplicas = getLeadersOfFollowerReplicasOnBroker(broker, topic, clusterModel);
        Collection<Replica> otherBrokerLeadReplicas = getLeadReplicasOnOtherBrokers(broker, topic, clusterModel);

        int numMoves = 0;

        while (numMoves < numLeadReplicasToMove && !(otherBrokerLeadReplicas.isEmpty() && followerReplicas.isEmpty())) {
            if (
                    attemptAssumeLeadershipAction(
                            clusterModel,
                            optimizedGoals,
                            optimizationOptions,
                            broker,
                            followerReplicas)
                    || attemptGainReplicaAction(
                            clusterModel,
                            optimizedGoals,
                            optimizationOptions,
                            broker,
                            otherBrokerLeadReplicas)
            ) {
                numMoves++;
                LOG.info("Lead replica gained for broker {} ({}/{} moves found)", broker.id(), numMoves, numLeadReplicasToMove);
            }
        }

        return numMoves;
    }

    private <T> T selectRandomFrom(Collection<T> collection) {
        return collection.isEmpty()
                ? null
                : collection.stream().skip(new Random().nextInt(collection.size())).findFirst().orElse(null);
    }

    private int loseLeadReplicas(
            Broker broker,
            String topic,
            int numLeadReplicasToMove,
            Collection<Replica> leaderReplicas,
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) {
        // Some partition counts won't divide evenly into the broker count so we allow for +1 differences (which
        // results in -1 fewer moves).
        int numExpectedMoves = (numLeadReplicasToMove * -1) - 1;

        int numMoves = 0;

        while (numMoves < numExpectedMoves && !leaderReplicas.isEmpty()) {
            Replica leaderReplica = selectRandomFrom(leaderReplicas);

            if (leaderReplica != null) {
                Set<Broker> candidates = getBrokersUnderCapacityForTopic(topic, clusterModel);

                if (
                        attemptRelinquishLeadershipAction(
                                clusterModel,
                                optimizedGoals,
                                optimizationOptions,
                                leaderReplica,
                                candidates)
                                || attemptRelinquishReplicaAction(
                                clusterModel,
                                optimizedGoals,
                                optimizationOptions,
                                leaderReplica,
                                candidates)
                ) {
                    numMoves++;
                    LOG.info("Lead replica relinquished for broker {} ({}/{} moves found)", broker.id(), numMoves, numExpectedMoves);
                }

                // Whether or not we're successful moving leadership of this replica, we don't want to attempt it
                // again.
                leaderReplicas.remove(leaderReplica);
            }
        }

        return numMoves;
    }

    // Return leader replicas for partitions that:
    //  a. Have a follower replica on the current broker
    //  b. Has a leader that is leading too many partitions of the current topic
    private Set<Replica> getLeadersOfFollowerReplicasOnBroker(Broker broker, String topic, ClusterModel clusterModel) {
        Set<Replica> replicas = new HashSet<>();

        for (Partition p : clusterModel.getPartitionsByTopic().get(topic)) {
            if (!p.leader().broker().equals(broker)) {
                int numLedPartitions = getNumLedPartitionsByLeader(p);

                if (numLedPartitions > _targetNumLeadReplicasByTopic.get(topic)) {
                    for (Replica r : p.followers()) {
                        if (r.broker().equals(broker)) {
                            replicas.add(getLeadReplicaOfPartition(clusterModel, r.topicPartition()));
                        }
                    }
                }
            }
        }

        return replicas;
    }

    // Return lead replicas for partitions that:
    //  a. Do not reside on the current broker
    //  b. Resides on a broker that is leading too many partitions of the current topic
    //  c. Is an allowed broker
    private Set<Replica> getLeadReplicasOnOtherBrokers(Broker broker, String topic, ClusterModel clusterModel) {
        Set<Replica> replicas = new HashSet<>();

        for (Partition p : clusterModel.getPartitionsByTopic().get(topic)) {
            if (
                    p.leader().broker() != broker
                            && !p.followerBrokers().contains(broker)
                            && _allowedBrokerIds.contains(p.leader().broker().id())
            ) {
                int numLedPartitions = getNumLedPartitionsByLeader(p);

                if (numLedPartitions > _targetNumLeadReplicasByTopic.get(topic)) {
                    replicas.add(p.leader());
                }
            }
        }

        return replicas;
    }

    private int getNumLedPartitionsByLeader(Partition partition) {
        return Math.toIntExact(
                partition.leader().broker().replicasOfTopicInBroker(partition.topicPartition().topic())
                        .stream()
                        .filter(Replica::isLeader)
                        .count());
    }

    private Set<Broker> getBrokersUnderCapacityForTopic(String topic, ClusterModel clusterModel) {
        int target = _targetNumLeadReplicasByTopic.get(topic);
        Map<Integer, Integer> numLeadReplicasByBrokerId = _numLeadReplicasByTopicByBrokerId.get(topic);

        return clusterModel.aliveBrokers().stream()
                .filter(b -> _allowedBrokerIds.contains(b.id()))
                .filter(b -> numLeadReplicasByBrokerId.getOrDefault(b.id(), 0) < target + 1)
                .collect(Collectors.toSet());
    }

    // Attempt to assume leadership of a partition that this broker is already in the ISR of. We will remove any replica
    // we attempt from `followerReplicas` as we go in order to avoid trying the same replicas multiple times.
    private boolean attemptAssumeLeadershipAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Broker broker,
            Collection<Replica> followerReplicas) {
        return attemptGainingAction(
                clusterModel,
                optimizedGoals,
                optimizationOptions,
                broker,
                followerReplicas,
                ActionType.LEADERSHIP_MOVEMENT);
    }

    // Attempt to assume leadership of a partition that this broker is not in the ISR of. We will remove the replica we
    // attempt from `otherBrokerLeadReplicas` as we go in order to avoid trying the same replicas multiple times.
    private boolean attemptGainReplicaAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Broker broker,
            Collection<Replica> otherBrokerLeadReplicas) {
        return attemptGainingAction(
                clusterModel,
                optimizedGoals,
                optimizationOptions,
                broker,
                otherBrokerLeadReplicas,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
    }

    private Replica getLeadReplicaOfPartition(ClusterModel clusterModel, TopicPartition topicPartition) {
        return clusterModel.getPartitionsByTopic()
                .get(topicPartition.topic())
                .get(topicPartition.partition())
                .leader();
    }

    private boolean attemptGainingAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Broker destinationBroker,
            Collection<Replica> targetReplicas,
            ActionType actionType) {
        Replica replica = selectRandomFrom(targetReplicas);
        if (replica != null) {
            Broker sourceBroker = replica.broker();

            assert replica.isLeader();

            Broker b = maybeApplyBalancingAction(
                    clusterModel,
                    replica,
                    Collections.singleton(destinationBroker),
                    actionType,
                    optimizedGoals,
                    optimizationOptions);
            targetReplicas.remove(replica); // Whether or not the above succeeds, we don't want to retry it.

            if (b != null) {
                updateLeadReplicaCounts(replica, sourceBroker, destinationBroker);
                LOG.info("Broker {} becoming leader of {} via a {} move",
                        b.id(), replica.topicPartition().toString(), actionType.balancingAction());
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    private void updateLeadReplicaCounts(
            Replica replica,
            Broker sourceBroker,
            Broker destinationBroker) {
        Map<Integer, Integer> toUpdate = _numLeadReplicasByTopicByBrokerId.get(replica.topicPartition().topic());

        toUpdate.compute(destinationBroker.id(), (brokerId, numLeadReplicas)
                -> numLeadReplicas == null ? 1 : numLeadReplicas + 1);
        toUpdate.compute(sourceBroker.id(), (brokerId, numLeadReplicas) -> {
            assert numLeadReplicas != null;
            return numLeadReplicas - 1;
        });
    }

    private boolean attemptRelinquishLeadershipAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Replica leaderReplica,
            Set<Broker> candidates) {
        Broker sourceBroker = leaderReplica.broker();

        Broker destinationBroker = maybeApplyBalancingAction(
                clusterModel,
                leaderReplica,
                candidates,
                ActionType.LEADERSHIP_MOVEMENT,
                optimizedGoals,
                optimizationOptions);

        if (destinationBroker != null) {
            updateLeadReplicaCounts(leaderReplica, sourceBroker, destinationBroker);
            LOG.info("Broker {} becoming leader of {} via a {} move",
                    destinationBroker.id(),
                    leaderReplica.topicPartition().toString(),
                    ActionType.LEADERSHIP_MOVEMENT.balancingAction());
            return true;
        } else {
            return false;
        }
    }

    private boolean attemptRelinquishReplicaAction(
            ClusterModel clusterModel,
            Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions,
            Replica replica,
            Set<Broker> candidateBrokers) {
        Broker sourceBroker = replica.broker();

        Broker destinationBroker = maybeApplyBalancingAction(
                clusterModel,
                replica,
                candidateBrokers,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                optimizedGoals,
                optimizationOptions);

        if (destinationBroker != null) {
            updateLeadReplicaCounts(replica, sourceBroker, destinationBroker);
            LOG.info("Broker {} becoming leader of {} via a {} move",
                    destinationBroker.id(),
                    replica.topicPartition().toString(),
                    ActionType.INTER_BROKER_REPLICA_MOVEMENT.balancingAction());
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unused")
    private void prettyPrintCurrentDistribution(ClusterModel clusterModel, String topic) {
        Map<Integer, Integer> leaderCountsByBrokerId = new HashMap<>();

        for (Partition p : clusterModel.getPartitionsByTopic().get(topic)) {
            leaderCountsByBrokerId.compute(
                    p.leader().broker().id(),
                    (brokerId, numLeadReplicas) -> numLeadReplicas == null ? 1 : numLeadReplicas + 1);
        }

        List<Integer> brokerIds = clusterModel.aliveBrokers().stream()
                .map(Broker::id)
                .sorted(Integer::compareTo)
                .collect(Collectors.toList());

        System.out.printf("ClusterModel leadership distribution for %s:%n", topic);

        for (Integer brokerId : brokerIds) {
            System.out.printf("\t%s: %s%n", brokerId, leaderCountsByBrokerId.get(brokerId));
        }

        System.out.printf("Cached leadership distribution for %s:%n", topic);

        for (Integer brokerId : brokerIds) {
            System.out.printf("\t%s: %s%n", brokerId, _numLeadReplicasByTopicByBrokerId.get(topic).get(brokerId));
        }

        System.out.println();
    }
}
