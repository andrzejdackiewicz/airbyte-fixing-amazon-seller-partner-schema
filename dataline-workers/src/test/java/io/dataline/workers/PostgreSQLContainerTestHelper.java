/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.dataline.workers;

import com.fasterxml.jackson.databind.JsonNode;
import io.dataline.commons.json.Jsons;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class PostgreSQLContainerTestHelper {

  public static void runSqlScript(MountableFile file, PostgreSQLContainer db)
      throws IOException, InterruptedException {
    String scriptPath = "/etc/" + UUID.randomUUID().toString() + ".sql";
    db.copyFileToContainer(file, scriptPath);
    db.execInContainer(
        "psql", "-d", db.getDatabaseName(), "-U", db.getUsername(), "-a", "-f", scriptPath);
  }

  public static JsonNode getSingerTapConfig(PostgreSQLContainer db) {
    return getSingerTapConfig(
        db.getUsername(),
        db.getPassword(),
        db.getHost(),
        db.getDatabaseName(),
        String.valueOf(db.getFirstMappedPort()));
  }

  public static JsonNode getSingerTapConfig(String user,
                                            String password,
                                            String host,
                                            String dbname,
                                            String port) {
    Map<String, String> creds = new HashMap<>();
    creds.put("user", user);
    creds.put("password", password);
    creds.put("host", host);
    creds.put("dbname", dbname);
    creds.put("port", port);

    return Jsons.jsonNode(creds);
  }

}
