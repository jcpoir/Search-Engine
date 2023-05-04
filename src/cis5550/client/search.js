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

  console.log(searchValue)

  try {
    const itemsPerPage = 10;
    const startIndex = (page - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;

    // TODO: replace with correct url
    // // Replace the URL with your Java server's URL
    const serverUrl = 'http://localhost:8080';

    // // Use Axios to make a GET request to your Java server's API endpoint
    const response = await axios.get(`${serverUrl}/search`, {
      params: {
        query: encodeURIComponent(searchValue),
      },
    });

    let data = response.data
    // start for dummy generateion, remove after url inplemented
    // let data;
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

    const results = data.slice(startIndex, endIndex);
    const totalPages = Math.ceil(data.length / itemsPerPage);

    // Create a new unordered list to hold the search results
    const resultList = document.createElement("ul");

    // Loop through each search result and create a new list item for it
    results.forEach((result) => {
      // Create a new list item element
      const listItem = document.createElement("li");
      listItem.classList.add("search-result");

      // Create a new heading element for the search result title, with a link to the full article
      const title = document.createElement("h3");
      const link = document.createElement("a");
      link.href = result.url;
      link.textContent = result.url;
      title.textContent = result.title
      title.appendChild(link);

      // Create a new paragraph element for the search result description
      const description = document.createElement("p");
      description.textContent = result.preview;

      // Add the title and description to the list item
      listItem.appendChild(title);
      listItem.appendChild(description);

      // Add the list item to the unordered list of search results
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

const dummyDataLong = Array.from({ length: 200 }, (_, i) => {
  const page = Math.floor(i / 10) + 1;
  return {
    url: `https://example.com/result/${i + 1}`,
    page: `This is a dummy description for the result item ${page}`,
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
