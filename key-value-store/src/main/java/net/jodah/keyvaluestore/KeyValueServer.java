package net.jodah.keyvaluestore;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.keyvaluestore.operations.Delete;
import net.jodah.keyvaluestore.operations.EntryEvent;
import net.jodah.keyvaluestore.operations.Get;
import net.jodah.keyvaluestore.operations.Listen;
import net.jodah.keyvaluestore.operations.Put;
import net.jodah.keyvaluestore.operations.PutWithTtl;

public class KeyValueServer {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueServer.class);

  public static void main(String... args) throws Throwable {
    deleteLogs();
    Address address = new Address("localhost", 5000);

    // Start the server
    CopycatServer server = CopycatServer.builder(address)
        .withStateMachine(KeyValueStore::new)
        .withTransport(NettyTransport.builder().withThreads(4).build())
        .withStorage(Storage.builder().withDirectory(new File("logs")).withStorageLevel(StorageLevel.DISK).build())
        .build();

    server.bootstrap().thenAccept(srvr -> LOG.info(srvr + " has bootstrapped a cluster")).join();

    // Create a client
    CopycatClient client = CopycatClient.builder()
        .withTransport(NettyTransport.builder().withThreads(2).build())
        .build();

    // Connect to the server
    client.connect(address).join();

    // Submit operations
    client.submit(new Put("foo", "Hello world!")).thenRun(() -> LOG.info("Put succeeded")).join();
    client.submit(new Get("foo")).thenAccept(result -> LOG.info("foo is: " + result)).join();
    client.submit(new Delete("foo")).thenRun(() -> LOG.info("foo has been deleted")).join();

    // Install listeners
    client.submit(new Listen()).thenRun(() -> LOG.info("listener registered")).join();
    client.onEvent("put", (EntryEvent event) -> LOG.info("put event received for: " + event.key));
    client.onEvent("delete", (EntryEvent event) -> LOG.info("delete event received for: " + event.key));
    client.onEvent("expire", (EntryEvent event) -> LOG.info("TTL expired event received for: " + event.key));

    // Submit operations to trigger event listeners
    client.submit(new Put("foo", "bar")).join();
    client.submit(new Get("foo")).join();
    client.submit(new Delete("foo")).join();
    client.submit(new PutWithTtl("foo", "bar", 1000)).join();
  }

  private static void deleteLogs() throws Throwable {
    Files.walkFileTree(new File("logs").toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
}
