package net.sf.javailp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Loader class for native CPLEX binaries bundled within AP.
 *
 * @since 28.10.2016
 */
public final class NativeLoaderCplex {

	private static final String NIX_VERSION = "1263"; // Library version for unix,
														// src/main/resources/NIX_VERSION/x86-64_linux
	private static final String WIN_VERSION = "1262"; // Library version for windows,
														// src/main/resources/WIN_VERSION/x64_win64

	private static final String RESOURCESPATH = "lib/cplex";

	private static final String CPLEX_DLLSTART = "cplex";
	private static final String LIBCPLEX_SOSTART = "libcplex";
	private static String prefix = "CPLEXExtractedLib";
	private static String pathSep = System.getProperty("file.separator");
	private static final String JAVA_LIB_PATH = "java.library.path";

	private static boolean loaded = false;

	/*
	 * each private static is owned by a JVM, each JVM needs to load Cplex libs to
	 * run Cplex
	 */
	private static Object jvmSync = new Object();

	private NativeLoaderCplex() {
		super();
	}

	/**
	 * Load.
	 *
	 * @throws IOException
	 */
	public static void load() throws IOException {
		synchronized (jvmSync) {
			if (loaded) {
				return;
			}
			loaded = true;
			File tempDir = createTempDir(prefix);
			String osArch = System.getProperty("os.arch");
			String osName = System.getProperty("os.name").toLowerCase();
			List<String> libs = null;
			String path = null;

			if (osName.startsWith("win")) {
				if (osArch.contains("64")) {
					path = RESOURCESPATH + WIN_VERSION + "/x64_win64/";
					libs = Arrays.asList(CPLEX_DLLSTART + WIN_VERSION + ".dll",
							CPLEX_DLLSTART + WIN_VERSION + "processtransport.dll",
							CPLEX_DLLSTART + WIN_VERSION + "remote.dll", CPLEX_DLLSTART + WIN_VERSION + "remotejni.dll",
							CPLEX_DLLSTART + WIN_VERSION + "tcpiptransport.dll");
				}
			} else if (osName.startsWith("linux")) {
				path = RESOURCESPATH + NIX_VERSION + "/x86-64_linux/";
				libs = Arrays.asList(LIBCPLEX_SOSTART + NIX_VERSION + ".so",
						LIBCPLEX_SOSTART + NIX_VERSION + "mpitransport.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "mpiworker.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "processtransport.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "processworker.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "remote.so", LIBCPLEX_SOSTART + NIX_VERSION + "remotejni.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "tcpiptransport.so",
						LIBCPLEX_SOSTART + NIX_VERSION + "tcpipworker.so");
			}
			if (libs == null) {
				throw new UnsupportedOperationException("Platform " + osName + ":" + osArch + " not supported");
			}

			for (String lib : libs) {
				loadLibrary(tempDir, path, lib);
			}
			System.setProperty(JAVA_LIB_PATH, System.getProperty(JAVA_LIB_PATH) + ":" + tempDir.getAbsolutePath());
			for (String lib : libs) {
				System.load(getLibraryPath(tempDir, lib));
			}
		}
	}

	private static File createTempDir(final String prefix) {
		String tmpDirName = System.getProperty("java.io.tmpdir");
		if (tmpDirName.endsWith(pathSep)) {
			tmpDirName += prefix + System.nanoTime() + pathSep;
		} else {
			tmpDirName += pathSep + prefix + System.nanoTime() + pathSep;
		}
		File dir = new File(tmpDirName);
		if (dir.mkdir()) {
			dir.deleteOnExit();
		}
		return dir;
	}

	private static String getLibraryPath(final File dir, final String name) throws IOException {
		File file = new File(dir, name);
		return file.getAbsolutePath();
	}

	private static String loadLibrary(final File dir, final String path, final String name) throws IOException {
		try (InputStream in = NativeLoaderCplex.class.getClassLoader().getResourceAsStream(path + name)) {
			if (in == null) {
				throw new IOException("File " + path + name + " could not be found.");
			}
			File file = new File(dir, name);
			file.deleteOnExit();
			if (!file.createNewFile()) {
				throw new IOException("New file " + file + " could not be created.");
			}
			try (OutputStream out = new FileOutputStream(file)) {
				int cnt;
				byte[] buf = new byte[16 * 1024];
				while ((cnt = in.read(buf)) >= 1) {
					out.write(buf, 0, cnt);
				}
			}
			return file.getAbsolutePath();
		}
	}
}
