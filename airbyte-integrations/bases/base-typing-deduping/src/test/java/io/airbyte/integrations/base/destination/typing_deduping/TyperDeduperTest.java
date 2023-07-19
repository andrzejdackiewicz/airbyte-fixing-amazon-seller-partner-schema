package io.airbyte.integrations.base.destination.typing_deduping;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.airbyte.protocol.models.v0.DestinationSyncMode;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.internal.verification.NoInteractions;

public class TyperDeduperTest {

  private DestinationHandler<String> destinationHandler;
  private TyperDeduper<String> typerDeduper;
  private InOrder inOrder;

  @BeforeEach
  void setup() {
    SqlGenerator<String> sqlGenerator = new MockSqlGenerator();
    destinationHandler = mock(DestinationHandler.class);
    inOrder = inOrder(destinationHandler);
    ParsedCatalog parsedCatalog = new ParsedCatalog(List.of(
        new StreamConfig(
            new StreamId("overwrite_ns", "overwrite_stream",  null, null, "overwrite_ns", "overwrite_stream"),
            null,
            DestinationSyncMode.OVERWRITE,
            null,
            null,
            null),
        new StreamConfig(
            new StreamId("append_ns", "append_stream",  null, null, "append_ns", "append_stream"),
            null,
            DestinationSyncMode.APPEND,
            null,
            null,
            null),
        new StreamConfig(
            new StreamId("dedup_ns", "dedup_stream",  null, null, "dedup_ns", "dedup_stream"),
            null,
            DestinationSyncMode.APPEND_DEDUP,
            null,
            null,
            null)));

    typerDeduper = new TyperDeduper<>(sqlGenerator, destinationHandler, parsedCatalog);
  }

  /**
   * When there are no existing tables, we should create them and write to them directly.
   */
  @Test
  void emptyDestination() throws Exception {
    when(destinationHandler.findExistingTable(any())).thenReturn(Optional.empty());

    typerDeduper.createFinalTables();
    inOrder.verify(destinationHandler).execute("CREATE TABLE overwrite_ns.overwrite_stream");
    inOrder.verify(destinationHandler).execute("CREATE TABLE append_ns.append_stream");
    inOrder.verify(destinationHandler).execute("CREATE TABLE dedup_ns.dedup_stream");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.typeAndDedupe("overwrite_ns", "overwrite_stream");
    typerDeduper.typeAndDedupe("append_ns", "append_stream");
    typerDeduper.typeAndDedupe("dedup_ns", "dedup_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE overwrite_ns.overwrite_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE append_ns.append_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE dedup_ns.dedup_stream");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.commitFinalTables();
    verify(destinationHandler, new NoInteractions()).execute(any());
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
  }

  /**
   * When there's an existing table but it's empty, we should ensure it has the right schema and
   * write to it directly.
   */
  @Test
  void existingEmptyTable() throws Exception {
    when(destinationHandler.findExistingTable(any())).thenReturn(Optional.of("foo"));
    when(destinationHandler.isFinalTableEmpty(any())).thenReturn(true);

    typerDeduper.createFinalTables();

    inOrder = inOrder(destinationHandler);
    inOrder.verify(destinationHandler).execute("ALTER TABLE overwrite_ns.overwrite_stream WITH EXISTING foo");
    inOrder.verify(destinationHandler).execute("ALTER TABLE append_ns.append_stream WITH EXISTING foo");
    inOrder.verify(destinationHandler).execute("ALTER TABLE dedup_ns.dedup_stream WITH EXISTING foo");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.typeAndDedupe("overwrite_ns", "overwrite_stream");
    typerDeduper.typeAndDedupe("append_ns", "append_stream");
    typerDeduper.typeAndDedupe("dedup_ns", "dedup_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE overwrite_ns.overwrite_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE append_ns.append_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE dedup_ns.dedup_stream");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.commitFinalTables();
    verify(destinationHandler, new NoInteractions()).execute(any());
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
  }

  /**
   * When there's an existing table nonempty table, we should alter it. For the OVERWRITE stream, we
   * also need to write to a tmp table, and overwrite the real table at the end of the sync.
   */
  @Test
  void existingNonemptyTable() throws Exception {
    when(destinationHandler.findExistingTable(any())).thenReturn(Optional.of("foo"));
    when(destinationHandler.isFinalTableEmpty(any())).thenReturn(false);

    typerDeduper.createFinalTables();

    inOrder = inOrder(destinationHandler);
    // NB: We only create one tmp table here.
    // Also, we need to alter the existing _real_ table, not the tmp table!
    inOrder.verify(destinationHandler).execute("ALTER TABLE overwrite_ns.overwrite_stream WITH EXISTING foo");
    inOrder.verify(destinationHandler).execute("CREATE TABLE overwrite_ns.overwrite_stream_airbyte_tmp");
    inOrder.verify(destinationHandler).execute("ALTER TABLE append_ns.append_stream WITH EXISTING foo");
    inOrder.verify(destinationHandler).execute("ALTER TABLE dedup_ns.dedup_stream WITH EXISTING foo");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.typeAndDedupe("overwrite_ns", "overwrite_stream");
    typerDeduper.typeAndDedupe("append_ns", "append_stream");
    typerDeduper.typeAndDedupe("dedup_ns", "dedup_stream");
    // NB: no airbyte_tmp suffix on the non-overwrite streams
    inOrder.verify(destinationHandler).execute("UPDATE TABLE overwrite_ns.overwrite_stream_airbyte_tmp");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE append_ns.append_stream");
    inOrder.verify(destinationHandler).execute("UPDATE TABLE dedup_ns.dedup_stream");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
    clearInvocations(destinationHandler);

    typerDeduper.commitFinalTables();
    verify(destinationHandler).execute("OVERWRITE TABLE overwrite_ns.overwrite_stream FROM SUFFIX _airbyte_tmp");
    verifyNoMoreInteractions(ignoreStubs(destinationHandler));
  }

  @Test
  void nonexistentStream() {
    assertThrows(IllegalArgumentException.class,
        () -> typerDeduper.typeAndDedupe("nonexistent_ns", "nonexistent_stream"));
  }
}
