/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    IterableUtil.java
 * _____________________________________________________________________________ *
 * Copyright: (C) Daimler AG 2016, all rights reserved
 * _____________________________________________________________________________
 */
package dw.tools.hsg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;

import scala.Tuple2;

/**
 * Utility functions on Iterables, Lists, etc.
 *
 * @author wirtzd
 * @since 04.05.2016
 */
public final class IterableUtil {

	/**
	 * Instantiates a new list util.
	 */
	private IterableUtil() {
		super();
	}

	/**
	 * Broadcast map.
	 *
	 * @param       <K> the key type
	 * @param       <V> the value type
	 * @param input the input
	 * @param ctx   the ctx
	 * @return the broadcast
	 */
	public static <K, V> Broadcast<Map<K, V>> bcMap(final Map<K, V> input, final JavaSparkContext ctx) {
		return ctx.broadcast(new HashMap<>(input));
	}

	/**
	 * Extracts a list of the first elements of a Tuple list.
	 *
	 * @param      <T> the generic type
	 * @param      <U> the generic type
	 * @param list the list
	 * @return the list of first elems
	 */
	public static <T, U> List<T> getListOfFirstElems(final Iterable<Tuple2<T, U>> list) {
		List<T> res = new ArrayList<>();
		for (Tuple2<T, U> entry : list) {
			res.add(entry._1);
		}
		return res;
	}

	/**
	 * Extracts a list of the second elements of a Tuple list.
	 *
	 * @param      <T> the generic type
	 * @param      <U> the generic type
	 * @param list the list
	 * @return the list of second elems
	 */
	public static <T, U> List<U> getListOfSecondElems(final Iterable<Tuple2<T, U>> list) {
		List<U> res = new ArrayList<>();
		for (Tuple2<T, U> entry : list) {
			res.add(entry._2);
		}
		return res;
	}

	/**
	 * Takes an iterable of Optional<> elements and returns a list of all elements
	 * that are present.
	 *
	 * @param      <T> the generic type
	 * @param list the list
	 * @return the all present
	 */
	public static <T> List<T> getAllPresent(final Iterable<Optional<T>> list) {
		List<T> res = new ArrayList<>();
		for (Optional<T> opt : list) {
			if (opt.isPresent()) {
				res.add(opt.get());
			}
		}
		return res;
	}

	/**
	 * Counts the elements in a generic Iterable.
	 *
	 * @param       <T> the generic type
	 * @param elems the elems
	 * @return the int
	 */
	public static <T> int count(final Iterable<T> elems) {
		int cnt = 0;
		Iterator<T> it = elems.iterator();
		while (it.hasNext()) {
			cnt++;
			it.next();
		}
		return cnt;
	}

	/**
	 * Transforms the iterable into a list.
	 *
	 * @param       <T> the generic type
	 * @param elems the elems
	 * @return the list
	 */
	public static <T> List<T> toList(final Iterable<T> elems) {
		if (elems instanceof ArrayList) {
			return (ArrayList<T>) elems;
		} else if (elems instanceof List) {
			return new ArrayList<>((List<T>) elems);
		} else if (elems instanceof Collection) {
			return new ArrayList<>((Collection<T>) elems);
		}
		List<T> res = new ArrayList<>();
		elems.forEach(res::add);
		return res;
	}

	/**
	 * Transforms the iterable into a map.
	 *
	 * @param       <K> the key type
	 * @param       <V> the value type
	 * @param elems the elems
	 * @return the map
	 */
	public static <K, V> Map<K, V> toMap(final Iterable<Tuple2<K, V>> elems) {
		Map<K, V> res = new HashMap<>();
		elems.forEach(t -> res.put(t._1, t._2));
		return res;
	}

	/**
	 * Transforms the iterable into a set.
	 *
	 * @param       <T> the generic type
	 * @param elems the elems
	 * @return the set
	 */
	public static <T> Set<T> toSet(final Iterable<T> elems) {
		if (elems instanceof Set) {
			return (Set<T>) elems;
		} else if (elems instanceof Collection) {
			return new HashSet<>((Collection<T>) elems);
		}
		Set<T> res = new HashSet<>();
		elems.forEach(res::add);
		return res;
	}

	/**
	 * Joins multiple Iterables into one Iterable of the same type.
	 *
	 * @param             <E> the element type
	 * @param iteratorsIn the iterators in
	 * @return the iterable
	 */
	@SuppressWarnings("squid:S1604") // lambda does not work with kryo
	public static <E> Iterable<E> union(final Iterable<Iterable<E>> iteratorsIn) {
		final Queue<Iterator<E>> queue = new LinkedList<>();
		iteratorsIn.forEach(it -> queue.add(it.iterator()));
		return new Iterable<E>() { // lambda will break kryo
			@Override
			public Iterator<E> iterator() {
				return new AbstractIterator<E>() {

					@Override
					protected E computeNext() {
						while (!queue.isEmpty()) {
							Iterator<E> topIter = queue.poll();
							if (topIter.hasNext()) {
								E result = topIter.next();
								queue.offer(topIter);
								return result;
							}
						}
						return endOfData();
					}
				};
			}
		};
	}

	/**
	 * Converts a Stream to a Java iterable
	 *
	 * @param   <T> the generic type
	 * @param s the s
	 * @return the iterable
	 */
	@SuppressWarnings("squid:S1604") // lambda does not work with kryo!
	public static <T> Iterable<T> streamIterator(final Stream<T> s) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return s.iterator();
			}
		};
	}
}
