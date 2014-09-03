/**
 * Copyright 2014 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.curator.test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * 
 * 
 * @author gcommeau
 */
public class MyZkTestServer implements Closeable {
	private static final File TEMP_DIR = new File("src/test/data");
	private static final File LOG_DIR = new File(TEMP_DIR, "testLog");
	private static final File DATA_DIR = new File(TEMP_DIR, "testData");

	public ZooKeeperServer server;
	private final int port;
	private Object factory;
	private final int tickTime;

	/**
	 * Create the server using the given port
	 * 
	 * @param port
	 *            the port
	 * @param tt
	 *            The tick time (in ms)
	 * @throws Exception
	 *             errors
	 */
	public MyZkTestServer(final int port, final int tt) throws Exception {
		this.port = port;
		this.tickTime = tt;
		clearDirs();
		start();
	}

	protected void clearDirs() {
		try {
			DirectoryUtils.deleteRecursively(LOG_DIR);
			DirectoryUtils.deleteRecursively(DATA_DIR);
		} catch (final IOException e) {
			// ignore
		}
	}

	/**
	 * Return the port being used
	 * 
	 * @return port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Returns the connection string to use
	 * 
	 * @return connection string
	 */
	public String getConnectString() {
		return "127.0.0.1:" + port;
	}

	/**
	 * Stop the server without deleting the temp directory
	 */
	public void stop() {
		final ZKDatabase zkDb = server.getZKDatabase();
		try {
			zkDb.close();
		} catch (final IOException e) {
			System.err.println("Error closing logs");
			e.printStackTrace();
		}
		server.shutdown();
		ServerHelper.shutdownFactory(factory);
	}

	/**
	 * Restarts the server (after stop)
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		server = new ZooKeeperServer(DATA_DIR, LOG_DIR, tickTime);
		factory = ServerHelper.makeFactory(server, port);
	}

	/**
	 * Close the server and any open clients and delete the temp directory
	 */
	@Override
	public void close() {
		try {
			stop();
		} finally {
			clearDirs();
		}
	}
}