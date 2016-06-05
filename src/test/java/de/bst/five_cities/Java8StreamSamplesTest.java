package de.bst.five_cities;

import static java.util.Arrays.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class Java8StreamSamplesTest {

	private List<String> data = asList("Valentina", "Alexander", "Reshma", "Sascha", "Tobias", "Udo", "Marcel",
			"Bjørn");

	@Test
	public void classicJava() {
		ArrayList<String> result = new ArrayList<>();
		for (String n : data) {
			String l = n.toLowerCase();
			if (l.length() > 5)
				result.add(l);
		}

		Collections.sort(result);
		for (String n : result)
			System.out.println(n);

	}

	@Test
	public void functionalJava() {
		data.stream() //
				.map(n -> n.toLowerCase()) //
				.filter(n -> n.length() > 5) //
				.sorted() //
				.forEach(n -> System.out.println(n));
	}

}
