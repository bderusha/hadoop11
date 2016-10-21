package code.lemma;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.stanford.nlp.simple.Sentence;

public class Tokenizer {

    // Stopwords from http://www.lextek.com/manuals/onix/stopwords1.html
    public static final String[] SET_VALUES = new String[] {
        "-", "th", "'s", ".", "px", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", "a","about","above","across","after","again","against","all","almost","alone","along","already","also","although","always","among","an","and","another","any","anybody","anyone","anything","anywhere","are","area","areas","around","as","ask","asked","asking","asks","at","away","b","back","backed","backing","backs","be","became","because","become","becomes","been","before","began","behind","being","beings","best","better","between","big","both","but","by","c","came","can","cannot","case","cases","certain","certainly","clear","clearly","come","could","d","did","differ","different","differently","do","does","done","down","down","downed","downing","downs","during","e","each","early","either","end","ended","ending","ends","enough","even","evenly","ever","every","everybody","everyone","everything","everywhere","f","face","faces","fact","facts","far","felt","few","find","finds","first","for","four","from","full","fully","further","furthered","furthering","furthers","g","gave","general","generally","get","gets","give","given","gives","go","going","good","goods","got","great","greater","greatest","group","grouped","grouping","groups","h","had","has","have","having","he","her","here","herself","high","high","high","higher","highest","him","himself","his","how","however","i","if","important","in","interest","interested","interesting","interests","into","is","it","its","itself","j","just","k","keep","keeps","kind","knew","know","known","knows","l","large","largely","last","later","latest","least","less","let","lets","like","likely","long","longer","longest","m","made","make","making","man","many","may","me","member","members","men","might","more","most","mostly","mr","mrs","much","must","my","myself","n","necessary","need","needed","needing","needs","never","new","new","newer","newest","next","no","nobody","non","noone","not","nothing","now","nowhere","number","numbers","o","of","off","often","old","older","oldest","on","once","one","only","open","opened","opening","opens","or","order","ordered","ordering","orders","other","others","our","out","over","p","part","parted","parting","parts","per","perhaps","place","places","point","pointed","pointing","points","possible","present","presented","presenting","presents","problem","problems","put","puts","q","quite","r","rather","really","right","right","room","rooms","s","said","same","saw","say","says","second","seconds","see","seem","seemed","seeming","seems","sees","several","shall","she","should","show","showed","showing","shows","side","sides","since","small","smaller","smallest","so","some","somebody","someone","something","somewhere","state","states","still","still","such","sure","t","take","taken","than","that","the","their","them","then","there","therefore","these","they","thing","things","think","thinks","this","those","though","thought","thoughts","three","through","thus","to","today","together","too","took","toward","turn","turned","turning","turns","two","u","under","until","up","upon","us","use","used","uses","v","very","w","want","wanted","wanting","wants","was","way","ways","we","well","wells","went","were","what","when","where","whether","which","while","who","whole","whose","why","will","with","within","without","work","worked","working","works","would","x","y","year","years","yet","you","young","younger","youngest","your","yours","z"
    };
    public static final Set<String> STOPWORD_SET = new HashSet<String>(Arrays.asList(SET_VALUES));
    public static final String WIKI_MARKUP_REGEX = "\\[\\[(File|Image):.*?\\]\\]|&lt;ref.*?/(ref|)&gt;|&[a-z]*?;|[^a-z^A-Z^À-ÿ^\\-^'^.]|'{2,5}";
    private final Stemmer stemmer = new Stemmer();


    public Tokenizer() {
        // TODO Auto-generated constructor stub
    }

    public List<String> tokenize(String text) {
    	// Clean up wiki markup
        text = text.replaceAll(WIKI_MARKUP_REGEX, " ");
        
    	BreakIterator iter = BreakIterator.getWordInstance(Locale.ENGLISH);
        iter.setText(text);
        LinkedList<String> stems = new LinkedList<String>();
        
        int lastIndex = iter.first();
        while (lastIndex != BreakIterator.DONE) {
            int firstIndex = lastIndex;
            lastIndex = iter.next();

            if (lastIndex != BreakIterator.DONE) {
                String word = text.substring(firstIndex, lastIndex);
                if (!word.matches("\\s+") && !STOPWORD_SET.contains(word.toLowerCase())) {
                    System.out.println(word.toLowerCase());
                    for (int i = 0; i < word.length(); i++) {
            	    	if (Character.isLetter(word.charAt(i))) {
            	  	    char ch = Character.toLowerCase(word.charAt(i));
            		    stemmer.add(ch);
            		}
            	    }
                    stemmer.stem();
                    word = stemmer.toString();
                	stems.add(word);
                }
            }
        }
        // Remove stopwords
        stems.removeAll(STOPWORD_SET);

        return stems;
    }
}
