var dumpster = require('dumpster-dive');
const {argv} = require('process')
let obj = {
  file: argv[2],
  db: argv[3]+"wiki",
  db_url: argv[4],
  skip_redirects: false,
  skip_disambig: true,
  infoboxes: false,
  links: true,
  categories: true,
  images: false,
  tables: false,
  coordinates: false,
  wikitext: false,
  templates: false,
  citations: false,
  domain: "wikipedia.org",
  url: true,
  lang: argv[3],
  isRedirect: true,
  redirectTo: true,
  list: false,
  batch_size:500,
  custom: function (doc) {
    //const sections =  doc.sections().map((i) => JSON.stringify({"text": i.text(), "title": i.title()}));
    function sectionMap(i) {
      return {
        text: i.text(),
        // Save by sections to avoid the need to convert from sentence to section text. Can be useful if one wants to do NLP by sentences.
        // Save links ONLY from sentences, to avoid saving links from the tables, info boxes, etc.
        links: i.sentences().map((s) => s.links().map((l) => l.json({encode:true}))).flat(),
        index: i.index(),
      }
    }
    function encode_key(i){
      key = i.title();
      if (key != ""){
        return key.replaceAll("\\", "\\\\").replaceAll("$", "\\u0024").replaceAll(".", "\\u002e");
      }
      // Abstract section has no title and index is 0
      if (i.index() == 0){
        return "Abstract";}
      // Bad section title, use index. Normally it's a subsection of a section missing its title...
      return "Section "+i.index();
    }
    var unorm = require('unorm');
    return {
      // Normalize to make it easier to search, keeping always first letter uppercase
      _id:  doc.title(),
      // Index by title to match li0nks implementation - link#section is always by section title
      sections: doc.sections().reduce((acc, i) => { acc[encode_key(i)] = sectionMap(i); return acc; }, {}),
      //categories: doc.categories(),
      isRedirect: doc.isRedirect(),
      redirectTo: doc.redirectTo(),
      url: doc.url(),
      title: unorm.nfkd(doc.title()),
      pageID: parseInt(doc.pageID()),

    };
  }

};
dumpster(obj, () => console.log('done!'));


