package ilog.cplex;

/**
 *
 * used to hide the original ilog.cplex CplexInitializer. we load our libs with
 * NativeLoaderCplex
 *
 * @author mattist
 * @since 10.11.2016
 */
@SuppressWarnings("ucd")
class CplexInitializer {
	private static String remoteLibraryPath = null;

	/**
	 * Instantiates a new cplex initializer.
	 */
	// "Hide this public constructor": no, it's needed
	@SuppressWarnings("squid:S1118")
	public CplexInitializer() {
		super();
	}

	public static String getRemoteLibraryPath() {
		return remoteLibraryPath;
	}
}
