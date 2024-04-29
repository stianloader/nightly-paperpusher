package org.stianloader.paperpusher.wiki;

import java.nio.file.Path;

public record WikiConfiguration(Path wikiInputPath, Path wikiOutputPath, String wikiBindPrefix) {

}
