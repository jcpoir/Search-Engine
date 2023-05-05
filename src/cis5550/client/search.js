async function categoryClicked(event, category) {
  await search(event, 1, true, category);
}

async function search(event, page = 1, isCategory = false, category = "") {
  var searchValue;

  // check if category link is clicked
  // if som search value = category value
  if (isCategory) {
    searchValue = category;
  } else {
    // if its a normal search
    // Get the user's search query from the form input field
    searchValue = document.getElementById("query").value;
  }
  event.preventDefault();

  // Clear any existing search results from the page
  const searchResults = document.getElementById("search-results");
  while (searchResults.firstChild) {
    searchResults.removeChild(searchResults.firstChild);
  }

  try {
    const itemsPerPage = 10;
    const startIndex = (page - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;

    // TODO: replace with correct url
    // Replace the URL with your Java server's URL
    const serverUrl = 'http://localhost:8080';

    // Use Axios to make a GET request to your Java server's API endpoint
    const response = await axios.get(`${serverUrl}/search`, {
      params: {
        query: encodeURIComponent(searchValue),
      },
    });
    const spellcheck = response.data.spellcheck;
    let data = response.data.results

    // start for dummy generateion, remove after url inplemented

    // let data data = dummyNew.results;
    
    // if (searchValue == "2") {
    //   data = dummyData2;
    // } else if (searchValue == "3") {
    //   data = dummyData3;
    // } else if (searchValue == "long") {
    //   data = dummyDataLong;
    // } else if (searchValue == "books") {
    //   data = dummyDataBooks;
    // } else if (searchValue == "music") {
    //   data = dummyDataMusic;
    // } else if (searchValue == "sports") {
    //   data = dummyDataSports;
    // } else if (searchValue == "travel") {
    //   data = dummyDataTravel;
    // } else {
    //   data = dummyData1;
    // }
    // end for dummy generateion


    const messageElement = document.getElementById("search-results-message");
    messageElement.innerHTML = "";

    if (spellcheck) {
      const message = `Showing search results for "${spellcheck}"`;
      messageElement.innerHTML = message;
      console.log('why', messageElement.innerHTML)
    }

    const results = data.slice(startIndex, endIndex);
    const totalPages = Math.ceil(data.length / itemsPerPage);

    // Create a new unordered list to hold the search results
    const resultList = document.createElement("ul");

    // Loop through each search result and create a new list item for it
    results.forEach((result) => {
      // Create a new div element for the search result
      const div = document.createElement("div");
      div.classList.add("search-result");
    
      // Create a new heading element for the search result domain, with a favicon
      const domain = document.createElement("h3");
      const domainLink = document.createElement("a");
      const favicon = document.createElement("img");
      const domainUrl = new URL(result.url);
      
      domainLink.href = result.url;
      domainLink.textContent = result.domain;
      domain.appendChild(favicon);
      domain.appendChild(domainLink);
    
      // Set the favicon source based on the domain of the search result
      favicon.src = `https://s2.googleusercontent.com/s2/favicons?domain=${domainUrl.hostname}`;
    
      // Create a new paragraph element for the search result title, with a link to the full article
      const title = document.createElement("p");
      const link = document.createElement("a");
      link.href = result.url;
      link.textContent = result.title;
      title.appendChild(link);
    
      // Create a new paragraph element for the search result URL
      const url = document.createElement("p");
      url.textContent = result.url;
    
      // Add the domain, title, and URL to the search result div
      div.appendChild(domain);
      div.appendChild(title);
      div.appendChild(url);
    
      // Add the search result div to the unordered list of search results
      const listItem = document.createElement("li");
      listItem.appendChild(div);
      resultList.appendChild(listItem);
    });
    

    // Add the unordered list of search results to the page
    searchResults.appendChild(resultList);

    // Create and add page navigation buttons
    const nav = document.createElement("nav");
    for (let i = 1; i <= totalPages; i++) {
      const button = document.createElement("button");
      button.textContent = i;
      button.classList.add("page-button");

      if (i === page) {
        button.disabled = true;
      }

      button.addEventListener("click", (event) => search(event, i));
      nav.appendChild(button);
    }
    searchResults.appendChild(nav);
  } catch (error) {
    // If there's an error fetching search results, display an error message on the page
    console.error("Error fetching search results:", error);
    searchResults.innerHTML =
      "<p>There was an error fetching search results. Please try again later.</p>";
  }
}

const dummyNew = {
  "spellcheck": "brown basketball",
  "results": [
     {
        "title": "\n\tMen\u0027s Basketball Club 2021-2022 - Brown University Recreation\n",
        "url": "https://brownrec.com:443/sports/2021/7/7/mens-basketball-club-2021-2022",
        "domain" : "https://brownrec.com"
     },
     {
        "title": "\n\tMen\u0027s Basketball - Brown University Athletics\n",
        "url": "https://brownbears.com:443/sports/mens-basketball",
        "domain" : "https://brownbears.com"
     },
     {
        "title": "\n\t2017-18 Men\u0027s Basketball Roster - Brown University Recreation\n",
        "url": "https://brownrec.com:443/sports/mens-basketball/roster/2017-18"
     },
     {
        "title": "\n\tMen\u0027s Basketball - Story Archives - Cornell University Athletics\n",
        "url": "https://cornellbigred.com:443/sports/mens-basketball/archives"
     },
     {
        "title": "\n\tWomen\u0027s Basketball - Story Archives - Cornell University Athletics\n",
        "url": "https://cornellbigred.com:443/sports/womens-basketball/archives"
     },
     {
        "title": "Hilary Silver’s homelessness documentary to premiere on PBS | News from Brown",
        "url": "https://news.brown.edu:443/articles/2015/11/homelessness"
     },
     {
        "title": "A better method for making perovskite solar cells | News from Brown",
        "url": "https://news.brown.edu:443/articles/2015/03/perovskite"
     },
     {
        "title": "Brown University honors veterans | News from Brown",
        "url": "https://news.brown.edu:443/articles/2015/11/veterans"
     },
     {
        "title": "Novel support for brain science theory with deep Brown roots | News from Brown",
        "url": "https://news.brown.edu:443/articles/2015/11/learning"
     },
     {
        "title": "Aging cells lose their grip on DNA rogues | News from Brown",
        "url": "https://news.brown.edu:443/articles/2013/01/senescence"
     }
  ]
}


const dummyDataLong = Array.from({ length: 200 }, (_, i) => {
  const page = Math.floor(i / 10) + 1;
  return {
    url: `https://example.com/result/${i + 1}`,
    title: `Dummy title for the result item ${page}, ${i + 1}`,
    description: `This is a dummy description for the result item ${page}, ${i + 1}: xxxxxxxxxxxxxxxxx...`,
  };
});

const dummyData1 = [];
for (let i = 1; i <= 5; i++) {
  const data = {
    url: `https://example.com/search/${i}`,
    page: `Here's a short description of the search result ${i}.`
  };
  dummyData1.push(data);
}

const dummyData2 = [];
for (let i = 1; i <= 5; i++) {
  const data = {
    url: `https://example.com/articles/${i}`,
    page: `This is a placeholder text for article ${i}.`
  };
  dummyData2.push(data);
}

const dummyData3 = [];
for (let i = 1; i <= 5; i++) {
  const data = {
    url: `https://example.com/guides/${i}`,
    page: `This is a placeholder text for guide ${i}.`
  };
  dummyData3.push(data);
}

const dummyDataBooks = [];
for (let i = 1; i <= 5; i++) {
  const data = {
    url: `https://example.com/books/${i}`,
    page: `This is a placeholder text for book ${i}.`
  };
  dummyDataBooks.push(data);
}

const dummyDataMusic = [];
for (let i = 1; i <= 5; i++) {
  dummyDataMusic.push({
    url: `https://example.com/music/${i}`,
    page: `This is a placeholder text for music ${i}.`,
  });
}

const dummyDataSports = [];
for (let i = 1; i <= 5; i++) {
  dummyDataSports.push({
    url: `https://example.com/sports/${i}`,
    page: `This is a placeholder text for sports ${i}.`,
  });
}

const dummyDataTravel = [];
for (let i = 1; i <= 5; i++) {
  dummyDataTravel.push({
    url: `https://example.com/travel/${i}`,
    page: `This is a placeholder text for travel ${i}.`,
  });
}
